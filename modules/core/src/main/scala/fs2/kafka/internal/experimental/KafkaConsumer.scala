/*
 * Copyright 2018-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.experimental

import java.time.Duration
import java.util

import scala.collection.immutable.SortedSet
import scala.util.matching.Regex

import cats.*
import cats.data.*
import cats.effect.*
import cats.effect.std.*
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import fs2.concurrent.Topic
import fs2.kafka.*
import fs2.kafka.consumer.{KafkaCommit, KafkaConsume, KafkaSubscription, MkConsumer}
import fs2.kafka.instances.*
import fs2.kafka.internal.{LogEntry, Logging, WithConsumer}
import fs2.kafka.internal.converters.collection.*
import fs2.kafka.internal.experimental.KafkaConsumer.{Request, State, Status}
import fs2.kafka.internal.syntax.*

import org.apache.kafka.clients.consumer.{
  ConsumerConfig,
  ConsumerRebalanceListener,
  OffsetAndMetadata
}
import org.apache.kafka.common.TopicPartition

class KafkaConsumer[F[_], K, V](
  settings: ConsumerSettings[F, K, V],
  keyDeserializer: KeyDeserializer[F, K],
  valueDeserializer: ValueDeserializer[F, V],
  state: AtomicCell[F, State[F, K, V]],
  withConsumer: WithConsumer[F],
  requests: Queue[F, Request[F, K, V]],
  assignments: Topic[F, Map[TopicPartition, PartitionStream[F, K, V]]],
  dispatcher: Dispatcher[F]
)(implicit F: Async[F], logging: Logging[F], jitter: Jitter[F])
    extends KafkaConsume[F, K, V]
    with KafkaSubscription[F]
    with KafkaCommit[F] {

  private[this] val consumerGroupId: Option[String] =
    settings.properties.get(ConsumerConfig.GROUP_ID_CONFIG)

  private[this] val pollTimeout: Duration = settings.pollTimeout.toJava

  private[this] val consumerRebalanceListener: ConsumerRebalanceListener =
    new ConsumerRebalanceListener {

      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit =
        exec(partitions)(revoked)

      // TODO The pause flag in partitions [{}] will be removed due to revocation
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
      _.subscribe(
        topics.toList.asJava,
        consumerRebalanceListener
      ),
      LogEntry.SubscribedTopics(topics, _)
    )

  override def subscribe(regex: Regex): F[Unit] =
    subscribe(
      _.subscribe(
        regex.pattern,
        consumerRebalanceListener
      ),
      LogEntry.SubscribedPattern(regex.pattern, _)
    )

  private def subscribe(f: KafkaByteConsumer => Unit, log: State[F, K, V] => LogEntry): F[Unit] =
    state
      .evalUpdateAndGet { state =>
        transitionTo(state, Status.Subscribed) {
          withConsumer.blocking(f)
        }
      }
      .log(log)

  override def unsubscribe: F[Unit] =
    state
      .get
      .flatMap { state =>
        this
          .state
          .evalUpdateAndGet { state =>
            transitionTo(state, Status.Unsubscribed) {
              state.assigned.values.toList.traverse_(_.close) >>

              withConsumer.blocking(_.unsubscribe)
            }.as(State.empty)
          }
          .log(LogEntry.Unsubscribed(_))
          .whenA(state.status == Status.Unsubscribed) // We can unsubscribe as much times as we want
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
        assignmentsStream
          .map { assignment =>
            assignment.map { case (partition, stream) =>
              partition -> stream.create(settings.maxPrefetchBatches, clean(partition))
            }
          }
          .concurrently(
            Stream(
              Stream.fixedRateStartImmediately(settings.pollInterval).as(Request.Poll),
              Stream.fromQueueUnterminated(requests)
            ).parJoin(2).foreach(handle)
          )
      }
      .onFinalize(unsubscribe) // Ensure clean state after interruption/error

  private[this] def clean(partition: TopicPartition)(status: PartitionStream.Status): F[Unit] =
    state
      .updateAndGet(state => state.withoutAssignment(partition))
      .log(LogEntry.FinishedPartitionStream(partition, status, _))

  private[this] def handle(request: Request[F, K, V]): F[Unit] = request match {
    case Request.Poll              => handlePoll
    case r: Request.Fetch[F, K, V] => handleFetch(r)
  }

  private[this] def handleFetch(request: Request.Fetch[F, K, V]): F[Unit] =
    state.update { state =>
      val partition = request.partition
      val records   = request.records
      state.withFetch(partition, records)
      // TODO Descartar fetch para particiones no asignadas (o paradas)
    }

  private[this] val handlePoll: F[Unit] = pollForFetches >>= completeFetches

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
      .whenA(!records.isEmpty) // Keep same state if nothing returned from `poll`

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
    commitWithRecovery(
      offsets,
      withConsumer.blocking(_.commitSync(offsets.asJava, settings.commitTimeout.toJava))
    )

  private[this] def commitWithRecovery(
    offsets: Map[TopicPartition, OffsetAndMetadata],
    commit: F[Unit]
  ): F[Unit] =
    commit.handleErrorWith(settings.commitRecovery.recoverCommitWith(offsets, commit))

  override def stopConsuming: F[Unit] = F.blocking(println("Muerto")) *> unsubscribe // TODO Close `records` stream

  private[this] def transitionTo[A](state: State[F, K, V], status: Status)(
    f: F[A]
  ): F[State[F, K, V]] =
    if (state.status.isTransitionAllowed(status)) f.as(state.withStatus(status))
    else
      F.raiseError(
        new IllegalStateException(s"Invalid consumer transition from ${state.status} to $status")
      )

  private[this] def assigned(assigned: SortedSet[TopicPartition]): F[Unit] =
    state
      .evalUpdateAndGet { state =>
//            val add = assigned.filter() &~ state.assigned.keySet
        assigned
          .toList
          .foldM(state) { (state, partition) =>
            state.assigned.get(partition) match {
              case Some(paused) if assigned(partition) => paused.resume.as(state)
              case Some(_)                             => state.withoutAssignment(partition).pure
              case None =>
                PartitionStream(partition, requests).map(state.withAssignment(partition, _))
            }
          }
      }
      .flatTap { state =>
        assignments
          .publish1(state.assigned.filter { case (partition, _) => assigned(partition) })
          .void
      }
      .log(LogEntry.AssignedPartitions(assigned, _))

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
  private[this] def revoked(revoked: SortedSet[TopicPartition]): F[Unit] = {
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
      .flatMap(_ => state.get) // TODO Commit stashed offsets here
      .log(LogEntry.RevokedPartitions(revoked, _))
  }

  private[this] def lost(lost: SortedSet[TopicPartition]): F[Unit] =
    F.raiseError(new UnsupportedOperationException) // TODO

}

object KafkaConsumer {

  sealed abstract class Request[-F[_], -K, -V]

  object Request {

    final case object Poll extends Request[Any, Any, Any]

    final case class Fetch[F[_], K, V](
      partition: TopicPartition,
      records: Either[Throwable, Chunk[CommittableConsumerRecord[F, K, V]]] => Unit
    ) extends Request[F, K, V]

  }

  sealed trait Status { self =>

    // TODO Permitir suscribirse multiples veces?
    def isTransitionAllowed(status: Status): Boolean =
      (self, status) match {
        case (Status.Unsubscribed, Status.Subscribed)   => true
        case (Status.Subscribed, Status.Streaming)      => true
        case (Status.Subscribed, Status.Unsubscribed)   => true
        case (Status.Streaming, Status.Unsubscribed)    => true
        case (Status.Unsubscribed, Status.Unsubscribed) => true // FIXME
        case _                                          => false
      }

  }

  object Status {

    case object Unsubscribed extends Status
    case object Subscribed   extends Status
    case object Streaming    extends Status

    implicit val eq: Eq[Status] = Eq.fromUniversalEquals

  }

  final case class State[F[_], K, V](
    assigned: Map[TopicPartition, PartitionStream[F, K, V]],
    fetches: Map[
      TopicPartition,
      Either[Throwable, Chunk[CommittableConsumerRecord[F, K, V]]] => Unit
    ],
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
      records: Either[Throwable, Chunk[CommittableConsumerRecord[F, K, V]]] => Unit
    ): State[F, K, V] =
      copy(fetches = fetches + (partition -> records))

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
      requests     <- Resource.eval(Queue.unbounded[F, Request[F, K, V]])
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
                        requests,
                        assignments,
                        dispatcher
                      )
                    )
                  )(_.stopConsuming) // TODO Testear resource.release graceful (cuando no se interrumpe el stream directamente)
    } yield consumer
  }

}
