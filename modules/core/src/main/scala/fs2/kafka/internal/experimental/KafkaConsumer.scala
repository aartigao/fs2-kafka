/*
 * Copyright 2018-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.experimental

import java.time.Duration
import java.util

import scala.collection.immutable.SortedSet
import scala.concurrent.duration.FiniteDuration
import scala.util.matching.Regex

import cats.*
import cats.data.NonEmptySet
import cats.effect.*
import cats.effect.std.*
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import fs2.concurrent.Topic
import fs2.kafka.*
import fs2.kafka.consumer.{
  KafkaAssignment,
  KafkaCommit,
  KafkaConsume,
  KafkaConsumeChunk,
  KafkaConsumerLifecycle,
  KafkaMetrics,
  KafkaOffsetsV2,
  KafkaSubscription,
  KafkaTopicsV2,
  MkConsumer
}
import fs2.kafka.instances.*
import fs2.kafka.internal.{LogEntry, Logging, WithConsumer}
import fs2.kafka.internal.converters.collection.*
import fs2.kafka.internal.experimental.KafkaConsumer.{State, Status}
import fs2.kafka.internal.syntax.*

import org.apache.kafka.clients.consumer.{
  ConsumerConfig,
  ConsumerRebalanceListener,
  OffsetAndMetadata,
  OffsetAndTimestamp
}
import org.apache.kafka.common.{Metric, MetricName, PartitionInfo, TopicPartition}

class KafkaConsumer[F[_], K, V](
  settings: ConsumerSettings[F, K, V],
  keyDeserializer: KeyDeserializer[F, K],
  valueDeserializer: ValueDeserializer[F, V],
  state: AtomicCell[F, State[F, K, V]],
  withConsumer: WithConsumer[F],
  assignments: Topic[F, Map[TopicPartition, PartitionStream[F, K, V]]],
  dispatcher: Dispatcher[F]
)(implicit F: Async[F], logging: Logging[F], jitter: Jitter[F])
    extends KafkaConsume[F, K, V]
    with KafkaConsumeChunk[F, K, V]
    with KafkaAssignment[F]
    with KafkaOffsetsV2[F]
    with KafkaSubscription[F]
    with KafkaTopicsV2[F]
    with KafkaCommit[F]
    with KafkaMetrics[F]
    with KafkaConsumerLifecycle[F] {

  private[this] val consumerGroupId: Option[String] =
    settings.properties.get(ConsumerConfig.GROUP_ID_CONFIG)

  private[this] val pollTimeout: Duration = settings.pollTimeout.toJava

  private[this] def rebalanceListener(consumer: KafkaByteConsumer): ConsumerRebalanceListener =
    new ConsumerRebalanceListener {

      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit =
        exec(partitions)(revoked(consumer))

      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit =
        exec(partitions)(assigned)

      override def onPartitionsLost(partitions: util.Collection[TopicPartition]): Unit =
        exec(partitions)(lost)

      @inline private def exec(
        partitions: util.Collection[TopicPartition]
      )(f: SortedSet[TopicPartition] => F[Unit]): Unit =
        dispatcher.unsafeRunSync(f(partitions.toSortedSet))

    }

  override def subscribe[G[_]: Reducible](topics: G[String]): F[Unit] =
    subscribe(
      (consumer, listener) => consumer.subscribe(topics.toList.asJava, listener),
      LogEntry.SubscribedTopics(topics, _)
    )

  override def subscribe(regex: Regex): F[Unit] =
    subscribe(
      (consumer, listener) => consumer.subscribe(regex.pattern, listener),
      LogEntry.SubscribedPattern(regex.pattern, _)
    )

  private def subscribe(
    f: (KafkaByteConsumer, ConsumerRebalanceListener) => Unit,
    log: State[F, K, V] => LogEntry
  ): F[Unit] =
    transitionToWith(Status.Subscribed) {
      withConsumer.blocking { consumer =>
        f(consumer, rebalanceListener(consumer))
      }
    }.log(log)

  override def unsubscribe: F[Unit] =
    state
      .get
      .flatMap { state =>
        withConsumer
          .blocking(_.unsubscribe)
          .productR(transitionTo(Status.Unsubscribed))
          .log(LogEntry.Unsubscribed(_))
          .whenA(state.status == Status.Subscribed) // We can unsubscribe as much times as we want
      }

  override def stream: Stream[F, CommittableConsumerRecord[F, K, V]] =
    partitionedStream.parJoinUnbounded

  override def partitionedStream: Stream[F, Stream[F, CommittableConsumerRecord[F, K, V]]] =
    partitionsMapStream.flatMap(partitionsMap => Stream.iterable(partitionsMap.values))

  override def partitionsMapStream
    : Stream[F, Map[TopicPartition, Stream[F, CommittableConsumerRecord[F, K, V]]]] =
    Stream
      .resource(assignments.subscribeAwaitUnbounded)
      .flatMap { assignmentsStream =>
        Stream.exec(transitionTo(Status.Streaming).void) ++
          assignmentsStream
            .map { assignment =>
              assignment.map { case (partition, stream) =>
                partition -> stream.create
              }
            }
            .concurrently(
              Stream.fixedRateStartImmediately(settings.pollInterval).foreach(_ => handlePoll)
            )
      }
      .onFinalize(unsubscribe) // Ensure clean state after interruption/error

  private[this] val handlePoll: F[Unit] =
    transitionTo(Status.Polling) >>
      pollForFetches.guarantee(transitionTo(Status.Streaming).void) >>=
      completeFetches

  private[this] def pollForFetches: F[KafkaByteConsumerRecords] =
    state
      .get
      .flatMap { state =>
        withConsumer.blocking { consumer =>
          val requested = state.fetches.keySet
          val paused    = consumer.paused.toSet
          val assigned  = consumer.assignment.toSet

          val resume = requested & paused
          val pause  = assigned &~ (requested | paused)

          if (resume.nonEmpty)
            consumer.resume(resume.asJava)

          // Here lies the streaming laziness magic that allows this library to backpressure records
          // If the partition stream has not requested a fetch, pausing that partition means `poll` will
          // not return records for it, thus, we only emit records for partitions that have asked for them
          if (pause.nonEmpty)
            consumer.pause(pause.asJava)

//          println(s"Pause: $pause")
//          println(s"Resume: $resume")
          consumer.poll(pollTimeout)
        }
      }

  private[this] def completeFetches(records: KafkaByteConsumerRecords): F[Unit] =
    state
      .evalUpdate { state =>
        val requested   = state.fetches.keySet
        val returned    = records.partitions.toSet
        val solicited   = returned & requested
        val unsolicited = returned -- solicited

        // Instead of storing unsolicited records, we reposition the fetch offset, effectively discarding
        // the records. This should happen very rarely, only in the event of a rebalance.
        if (unsolicited.nonEmpty) {
          withConsumer.blocking { consumer =>
            unsolicited.foreach { partition =>
              records
                .records(partition)
                .toList
                .headOption
                .foreach { record =>
                  consumer.seek(partition, record.offset)
                }
            }
          }
        }

        committableConsumerRecords(records, solicited).map(state.completeFetches)
      }
      .whenA(!records.isEmpty) // Keep same state if nothing returned from poll

  private[this] def committableConsumerRecords(
    batch: KafkaByteConsumerRecords,
    solicited: Set[TopicPartition]
  ): F[List[(TopicPartition, Chunk[CommittableConsumerRecord[F, K, V]])]] =
    Stream
      .iterable(solicited)
      .parEvalMapUnorderedUnbounded { partition =>
        // Deserializing records is a CPU-demanding task (whether JSON/Avro/Protobuf is used)
        // For this reason we parallelize the deserialization of each partition-chunked records
        Chunk
          .from(batch.records(partition).toVector)
          .traverse { rec =>
            ConsumerRecord
              .fromJava(rec, keyDeserializer, valueDeserializer)
              .map(committableConsumerRecord(_, partition))
          }
          .map(partition -> _)
      }
      .compile
      .toList

  private[this] def committableConsumerRecord(
    record: ConsumerRecord[K, V],
    partition: TopicPartition
  ): CommittableConsumerRecord[F, K, V] =
    CommittableConsumerRecord(
      record = record,
      offset = CommittableOffset(
        topicPartition = partition,
        consumerGroupId = consumerGroupId,
        offsetAndMetadata = new OffsetAndMetadata(
          record.offset + 1L,
          settings.recordMetadata(record)
        ),
        commit = commitAsync
      )
    )

  override def commitAsync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
    ifStatus(Status.Polling)(commitAsyncStash(offsets), commitAsyncNow(offsets))

  private[this] def commitAsyncStash(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
    F.unit // TODO

  private[this] def commitAsyncNow(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
    commitWithRecovery(
      offsets,
      F.async { (cb: Either[Throwable, Unit] => Unit) =>
          withConsumer
            .blocking {
              _.commitAsync(
                offsets.asJava,
                (_, exception) => cb(Option(exception).toLeft(()))
              )
            }
            .handleErrorWith(e => F.delay(cb(Left(e))))
            .as(Some(F.unit))
        }
        .timeoutTo(
          settings.commitTimeout,
          F.defer(F.raiseError {
            CommitTimeoutException(
              settings.commitTimeout,
              offsets
            )
          })
        )
    )

  override def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
    ifStatus(Status.Polling)(commitSyncStash(offsets), commitSyncNow(offsets))

  private[this] def commitSyncStash(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
    F.unit // TODO

  private[this] def commitSyncNow(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
    commitWithRecovery(
      offsets,
      withConsumer.blocking(commitSyncWithConsumer(_, offsets))
    )

  private[this] def commitSyncWithConsumer(
    consumer: KafkaByteConsumer,
    offsets: Map[TopicPartition, OffsetAndMetadata]
  ): Unit =
    consumer.commitSync(offsets.asJava, settings.commitTimeout.toJava)

  private[this] def commitWithRecovery(
    offsets: Map[TopicPartition, OffsetAndMetadata],
    commit: F[Unit]
  ): F[Unit] =
    commit.handleErrorWith(settings.commitRecovery.recoverCommitWith(offsets, commit))

  override def stopConsuming: F[Unit] = unsubscribe

  private[this] def ifStatus[A](status: Status)(ifTrue: => F[A], ifFalse: => F[A]): F[A] = {
    // Use `evalModify` instead of `get` to avoid race conditions and ensure status
    // value don't change during the invocation of `ifTrue` or `ifFalse`
    state.evalModify { state =>
      val f = if (state.status == status) ifTrue else ifFalse
      f.map((state, _))
    }
  }

  private[this] def transitionTo(status: Status): F[State[F, K, V]] =
    transitionToWith(status)(F.unit)

  private[this] def transitionToWith[A](status: Status)(
    f: F[A]
  ): F[State[F, K, V]] =
    state.evalUpdateAndGet { state =>
      if (state.status.isTransitionAllowed(status)) f.as(state.withStatus(status))
      else
        F.raiseError(
          new IllegalStateException(s"Invalid consumer transition from ${state.status} to $status")
        )
    }

  private[this] def assigned(assigned: SortedSet[TopicPartition]): F[Unit] =
    state
      .evalUpdateAndGet { state =>
        assigned
          .toList
          .foldM(state) { (state, partition) =>
            PartitionStream(
              settings,
              storeFetchFor(partition),
              cleanAssignmentFor(partition)
            ).map(state.withAssignment(partition, _))
          }
      }
      .flatTap { state =>
        assignments
          .publish1(state.assigned.filter { case (partition, _) => assigned(partition) })
          .void
      }
      .log(LogEntry.AssignedPartitions(assigned, _))

  private[this] def storeFetchFor(
    partition: TopicPartition
  )(callback: PartitionStream.FetchCallback[F, K, V]): F[Unit] =
    state
      .updateAndGet(state => state.withFetch(partition, callback))
      .log(LogEntry.StoredFetch2(partition, callback, _))

  private[this] def cleanAssignmentFor(
    partition: TopicPartition
  )(status: PartitionStream.Status): F[Unit] =
    state
      .updateAndGet(state => state.withoutAssignment(partition))
      .log(LogEntry.FinishedPartitionStream(partition, status, _))

  /**
    * Depending on the partition assignor used, it will revoke all the partitions before reassigning
    * them (eager) or revoke only the ones reassigned to other consumer (cooperative).
    *
    * Anyways, a revoked partition won't be owned by the consumer until it's reassigned (even if
    * reassigned to the same consumer). The right choice here is to gracefully stop the stream,
    * waiting for all the pending records to be processed, then process the stashed commits.
    *
    * @see
    *   org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.RebalanceProtocol
    */
  private[this] def revoked(
    consumer: KafkaByteConsumer
  )(revoked: SortedSet[TopicPartition]): F[Unit] = {
    state
      .evalModify { state =>
        state
          .assigned
          .view
          .filterKeys(revoked)
          .values
          .toList
          .traverse(_.close)
          .map { cbClosingStreams =>
            val newState = state.voidFetches(revoked).withoutAssignments(revoked)
            (newState, cbClosingStreams)
          }
      }
      .flatMap(_.sequence_)
      .whenA(revoked.nonEmpty) >> // TODO Commit stashed offsets here
      state.get.log(LogEntry.RevokedPartitions(revoked, _))
  }

  private[this] def lost(lost: SortedSet[TopicPartition]): F[Unit] =
    F.raiseError(new UnsupportedOperationException) // TODO

  override def assignment: F[SortedSet[TopicPartition]] = ???

  override def assignmentStream: Stream[F, SortedSet[TopicPartition]] = ???

  override def assign(partitions: NonEmptySet[TopicPartition]): F[Unit] = ???

  override def assign(topic: String): F[Unit] =
    partitionsFor(topic)
      .map(partitionInfo => NonEmptySet.fromSet(SortedSet(partitionInfo.map(_.partition)*)))
      .flatMap(partitions => partitions.fold(F.unit)(assign(topic, _)))

  override def committed(
    partitions: Set[TopicPartition]
  ): F[Map[TopicPartition, OffsetAndMetadata]] =
    withConsumer.blocking { consumer =>
      consumer.committed(partitions.asJava).toMap
    }

  override def committed(
    partitions: Set[TopicPartition],
    timeout: FiniteDuration
  ): F[Map[TopicPartition, OffsetAndMetadata]] =
    withConsumer.blocking { consumer =>
      consumer.committed(partitions.asJava, timeout.toJava).toMap
    }

  override def listTopics: F[Map[String, List[PartitionInfo]]] =
    withConsumer.blocking { consumer =>
      consumer.listTopics.toMap.map { case (k, v) => (k, v.toList) }
    }

  override def listTopics(timeout: FiniteDuration): F[Map[String, List[PartitionInfo]]] =
    withConsumer.blocking { consumer =>
      consumer.listTopics(timeout.toJava).toMap.map { case (k, v) => (k, v.toList) }
    }

  override def offsetsForTimes(
    timestampsToSearch: Map[TopicPartition, Long]
  ): F[Map[TopicPartition, Option[OffsetAndTimestamp]]] =
    withConsumer.blocking { consumer =>
      consumer
        .offsetsForTimes(
          timestampsToSearch.asInstanceOf[Map[TopicPartition, java.lang.Long]].asJava
        )
        .toMapOptionValues // Convert empty/missing partition null values to None for more idiomatic scala
    }

  override def offsetsForTimes(
    timestampsToSearch: Map[TopicPartition, Long],
    timeout: FiniteDuration
  ): F[Map[TopicPartition, Option[OffsetAndTimestamp]]] =
    withConsumer.blocking { consumer =>
      consumer
        .offsetsForTimes(
          timestampsToSearch.asInstanceOf[Map[TopicPartition, java.lang.Long]].asJava,
          timeout.toJava
        )
        .toMapOptionValues // Convert empty/missing partition null values to None for more idiomatic scala
    }

  override def metrics: F[Map[MetricName, Metric]] =
    withConsumer.blocking(consumer => consumer.metrics.toMap)

  override def terminate: F[Unit] = ???

  override def awaitTermination: F[Unit] = ???

  override def seek(partition: TopicPartition, offset: Long): F[Unit] =
    withConsumer.blocking(consumer => consumer.seek(partition, offset))

  override def seekToBeginning[G[_]: Foldable](partitions: G[TopicPartition]): F[Unit] =
    withConsumer.blocking(consumer => consumer.seekToBeginning(partitions.asJava))

  override def seekToEnd[G[_]: Foldable](partitions: G[TopicPartition]): F[Unit] =
    withConsumer.blocking(consumer => consumer.seekToEnd(partitions.asJava))

  override def position(partition: TopicPartition): F[Long] =
    withConsumer.blocking(consumer => consumer.position(partition))

  override def position(partition: TopicPartition, timeout: FiniteDuration): F[Long] =
    withConsumer.blocking(consumer => consumer.position(partition, timeout.toJava))

  override def partitionsFor(topic: String): F[List[PartitionInfo]] =
    withConsumer.blocking(consumer => consumer.partitionsFor(topic).toList)

  override def partitionsFor(topic: String, timeout: FiniteDuration): F[List[PartitionInfo]] =
    withConsumer.blocking(consumer => consumer.partitionsFor(topic, timeout.toJava).toList)

  override def beginningOffsets(partitions: Set[TopicPartition]): F[Map[TopicPartition, Long]] =
    withConsumer.blocking { consumer =>
      consumer.beginningOffsets(partitions.asJava).toMap.asInstanceOf[Map[TopicPartition, Long]]
    }

  override def beginningOffsets(
    partitions: Set[TopicPartition],
    timeout: FiniteDuration
  ): F[Map[TopicPartition, Long]] =
    withConsumer.blocking { consumer =>
      consumer
        .beginningOffsets(partitions.asJava, timeout.toJava)
        .toMap
        .asInstanceOf[Map[TopicPartition, Long]]
    }

  override def endOffsets(partitions: Set[TopicPartition]): F[Map[TopicPartition, Long]] =
    withConsumer.blocking { consumer =>
      consumer.endOffsets(partitions.asJava).toMap.asInstanceOf[Map[TopicPartition, Long]]
    }

  override def endOffsets(
    partitions: Set[TopicPartition],
    timeout: FiniteDuration
  ): F[Map[TopicPartition, Long]] =
    withConsumer.blocking { consumer =>
      consumer
        .endOffsets(partitions.asJava, timeout.toJava)
        .toMap
        .asInstanceOf[Map[TopicPartition, Long]]
    }

}

object KafkaConsumer {

  type FetchCallback[F[_], K, V] =
    Either[Throwable, Chunk[CommittableConsumerRecord[F, K, V]]] => Unit

  sealed trait Status { self =>

    def isTransitionAllowed(status: Status): Boolean =
      (self, status) match {
        case (Status.Unsubscribed, Status.Subscribed) => true
        case (Status.Subscribed, Status.Streaming)    => true
        case (Status.Subscribed, Status.Unsubscribed) => true
        case (Status.Streaming, Status.Unsubscribed)  => true
        case (Status.Streaming, Status.Polling)       => true
        case (Status.Polling, Status.Streaming)       => true
        case _                                        => false
      }

  }

  object Status {

    case object Unsubscribed extends Status
    case object Subscribed   extends Status
    case object Streaming    extends Status
    case object Polling      extends Status

    implicit val eq: Eq[Status] = Eq.fromUniversalEquals

  }

  final case class State[F[_], K, V](
    assigned: Map[TopicPartition, PartitionStream[F, K, V]],
    fetches: Map[TopicPartition, PartitionStream.FetchCallback[F, K, V]],
    status: Status
  ) {

    def withAssignment(
      partition: TopicPartition,
      stream: PartitionStream[F, K, V]
    ): State[F, K, V] =
      if (assigned.contains(partition)) this else copy(assigned = assigned + (partition -> stream))

    def withoutAssignment(partition: TopicPartition): State[F, K, V] = {
      val isAssigned = assigned.contains(partition)
      val hasFetch   = fetches.contains(partition)
      if (!isAssigned && !hasFetch) this
      else {
        val _assigned = if (!isAssigned) assigned else assigned - partition
        val _fetches  = if (!hasFetch) fetches else fetches - partition
        copy(assigned = _assigned, fetches = _fetches)
      }
    }

    def withoutAssignments(partitions: Iterable[TopicPartition]): State[F, K, V] =
      partitions.foldLeft(this)(_ withoutAssignment _)

    def withFetch(
      partition: TopicPartition,
      callback: PartitionStream.FetchCallback[F, K, V]
    ): State[F, K, V] =
      if (assigned.contains(partition)) copy(fetches = fetches + (partition -> callback))
      else {
        callback(Right(Chunk.empty)) // TODO Document why
        this
      }

    def withoutFetches(partitions: Iterable[TopicPartition]): State[F, K, V] =
      copy(fetches = fetches -- partitions)

    def voidFetches(partitions: Iterable[TopicPartition]): State[F, K, V] =
      completeFetches(partitions.toList.map(_ -> Chunk.empty))

    def completeFetches(
      records: List[(TopicPartition, Chunk[CommittableConsumerRecord[F, K, V]])]
    ): State[F, K, V] = {
      val completed =
        records.mapFilter { case (partition, recs) =>
          fetches
            .get(partition)
            .map { cb =>
              cb(Right(recs))
              partition
            }
        }
      if (completed.isEmpty) this else copy(fetches = fetches -- completed)
    }

    def withStatus(status: Status): State[F, K, V] =
      if (this.status === status) this else copy(status = status)

  }

  object State {

    def empty[F[_], K, V]: State[F, K, V] =
      State(
        assigned = Map.empty,
        fetches = Map.empty,
        status = Status.Unsubscribed
      )

    implicit def show[F[_], K, V]: Show[State[F, K, V]] = Show.fromToString

  }

  def stream[F[_]: Async: MkConsumer, K, V](
    settings: ConsumerSettings[F, K, V]
  ): Stream[F, KafkaConsumer[F, K, V]] =
    Stream.resource(resource(settings))

  def resource[F[_], K, V](
    settings: ConsumerSettings[F, K, V]
  )(implicit
    F: Async[F],
    mk: MkConsumer[F]
  ): Resource[F, KafkaConsumer[F, K, V]] = {
    for {
      keyDeserializer   <- settings.keyDeserializer
      valueDeserializer <- settings.valueDeserializer
      case implicit0(logging: Logging[F]) <- Resource
        .eval(Logging.default[F](new Object().hashCode))
      case implicit0(jitter: Jitter[F]) <- Resource.eval(Jitter.default[F])
      dispatcher   <- Dispatcher.sequential[F]
      withConsumer <- WithConsumer(mk, settings)
      state        <- Resource.eval(AtomicCell[F].of(State.empty[F, K, V]))
      assignments  <- Resource.eval(Topic[F, Map[TopicPartition, PartitionStream[F, K, V]]])
      consumer <- Resource.make(
                    F.pure(
                      new KafkaConsumer(
                        settings,
                        keyDeserializer,
                        valueDeserializer,
                        state,
                        withConsumer,
                        assignments,
                        dispatcher
                      )
                    )
                  )(kc => assignments.close.void >> kc.stopConsuming) // TODO Testear resource.release graceful (cuando no se interrumpe el stream directamente)
    } yield consumer
  }

}
