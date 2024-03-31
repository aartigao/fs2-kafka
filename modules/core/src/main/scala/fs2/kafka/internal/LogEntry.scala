/*
 * Copyright 2018-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal

import java.util.regex.Pattern

import scala.collection.immutable.SortedSet

import cats.*
import cats.data.{Chain, NonEmptyList, NonEmptySet, NonEmptyVector}
import cats.syntax.all.*
import fs2.kafka.instances.*
import fs2.kafka.internal.experimental.PartitionStream
import fs2.kafka.internal.syntax.*
import fs2.kafka.internal.KafkaConsumerActor.*
import fs2.kafka.internal.LogLevel.*
import fs2.kafka.CommittableConsumerRecord
import fs2.Chunk

import org.apache.kafka.common.TopicPartition

sealed abstract private[kafka] class LogEntry {

  def level: LogLevel

  def message: String

}

private[kafka] object LogEntry {

  final case class FinishedPartitionStream[State: Show](
    partition: TopicPartition,
    status: PartitionStream.Status,
    state: State
  ) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String =
      show"Partition stream for $partition finished with status $status. Current state [$state]."

  }

  final case class SubscribedTopics[G[_]: Reducible, State: Show](
    topics: G[String],
    state: State
  ) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String =
      show"Consumer subscribed to topics [${topics.mkString_(", ")}]. Current state [$state]."

  }

  final case class ManuallyAssignedPartitions[F[_]](
    partitions: NonEmptySet[TopicPartition],
    state: State[F, ?, ?]
  ) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String =
      s"Consumer manually assigned partitions [${partitions.toList.mkString(", ")}]. Current state [$state]."

  }

  final case class SubscribedPattern[State: Show](
    pattern: Pattern,
    state: State
  ) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String =
      s"Consumer subscribed to pattern [$pattern]. Current state [${state.show}]."

  }

  final case class Unsubscribed[State: Show](
    state: State
  ) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String =
      show"Consumer unsubscribed from all partitions. Current state [$state]."

  }

  final case class StoredFetch[F[_], K, V](
    partition: TopicPartition,
    callback: ((Chunk[CommittableConsumerRecord[F, K, V]], FetchCompletedReason)) => F[Unit],
    state: State[F, K, V]
  ) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String =
      s"Stored fetch [$callback] for partition [$partition]. Current state [$state]."

  }

  final case class StoredOnRebalance[F[_]](
    onRebalance: OnRebalance[F],
    state: State[F, ?, ?]
  ) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String =
      s"Stored OnRebalance [$onRebalance]. Current state [$state]."

  }

  final case class AssignedPartitions[State: Show](
    partitions: SortedSet[TopicPartition],
    state: State
  ) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String =
      s"Assigned partitions [${partitions.mkString(", ")}]. Current state [${state.show}]."

  }

  final case class RevokedPartitions[State: Show](
    partitions: SortedSet[TopicPartition],
    state: State
  ) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String =
      s"Revoked partitions [${partitions.mkString(", ")}]. Current state [${state.show}]."

  }

  final case class CompletedFetchesWithRecords[F[_]](
    records: Records[F],
    state: State[F, ?, ?]
  ) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String =
      s"Completed fetches with records for partitions [${recordsString(records)}]. Current state [$state]."

  }

  final case class RevokedFetchesWithRecords[F[_]](
    records: Records[F],
    state: State[F, ?, ?]
  ) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String =
      s"Revoked fetches with records for partitions [${recordsString(records)}]. Current state [$state]."

  }

  final case class RevokedFetchesWithoutRecords[F[_]](
    partitions: Set[TopicPartition],
    state: State[F, ?, ?]
  ) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String =
      s"Revoked fetches without records for partitions [${partitions.mkString(", ")}]. Current state [$state]."

  }

  final case class RemovedRevokedRecords[F[_]](
    records: Records[F],
    state: State[F, ?, ?]
  ) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String =
      s"Removed revoked records for partitions [${recordsString(records)}]. Current state [$state]."

  }

  final case class StoredRecords[F[_]](
    records: Records[F],
    state: State[F, ?, ?]
  ) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String =
      s"Stored records for partitions [${recordsString(records)}]. Current state [$state]."

  }

  final case class RevokedPreviousFetch(
    partition: TopicPartition,
    streamId: StreamId
  ) extends LogEntry {

    override def level: LogLevel = Warn

    override def message: String =
      s"Revoked previous fetch for partition [$partition] in stream with id [$streamId]."

  }

  final case class StoredPendingCommit[F[_]](
    commit: Request.Commit[F],
    state: State[F, ?, ?]
  ) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String =
      s"Stored pending commit [$commit] as rebalance is in-progress. Current state [$state]."

  }

  final case class CommittedPendingCommits[F[_]](
    pendingCommits: Chain[Request.Commit[F]],
    state: State[F, ?, ?]
  ) extends LogEntry {

    override def level: LogLevel = Debug

    override def message: String =
      s"Committed pending commits [$pendingCommits]. Current state [$state]."

  }

  def recordsString[F[_]](
    records: Records[F]
  ): String =
    records
      .toList
      .sortBy { case (tp, _) => tp }
      .mkStringAppend { case (append, (tp, ms)) =>
        append(tp.show)
        append(" -> { first: ")
        append(ms.head.offset.offsetAndMetadata.show)
        append(", last: ")
        append(ms.last.offset.offsetAndMetadata.show)
        append(" }")
      }("", ", ", "")

  private[this] type Records[F[_]] =
    Map[TopicPartition, NonEmptyVector[CommittableConsumerRecord[F, ?, ?]]]

}
