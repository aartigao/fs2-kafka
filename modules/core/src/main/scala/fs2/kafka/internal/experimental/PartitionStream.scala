/*
 * Copyright 2018-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.experimental

import cats.*
import cats.effect.*
import cats.effect.std.*
import cats.syntax.all.*
import fs2.*
import fs2.concurrent.*
import fs2.kafka.internal.experimental.KafkaConsumer.Request
import fs2.kafka.internal.experimental.PartitionStream.Status
import fs2.kafka.CommittableConsumerRecord

import org.apache.kafka.common.TopicPartition

class PartitionStream[F[_], K, V](
  partition: TopicPartition,
  store: Request.Fetch[F, K, V] => F[Unit],
  status: SignallingRef[F, Status]
)(implicit F: Async[F]) {

  private val pauseSignal     = status.map(_ == Status.Paused)
  private val interruptSignal = status.map(_ == Status.Stopped)

  def create: Stream[F, CommittableConsumerRecord[F, K, V]] =
    Stream.exec(transitionTo(Status.Running)) ++
      status
        .continuous
        .takeWhile(_.isNotFinished)
        .evalMap { _ =>
          F.async[Chunk[CommittableConsumerRecord[F, K, V]]] { cb =>
            store(Request.Fetch(partition, cb)).as(Some(F.unit))
          }
        }
        .debugChunks()
        .unchunks
        .pauseWhen(pauseSignal)
        .interruptWhen(interruptSignal)
        .onFinalize(transitionTo(Status.Init).flatTap(_ => F.pure(println(s"Finalizing $this"))))

  def isInit: F[Boolean] = status.get.map(_ == Status.Init)

  def pause: F[Unit] = transitionTo(Status.Paused)

  def isPaused: F[Boolean] = status.get.map(_ == Status.Paused)

  def resume: F[Unit] = transitionTo(Status.Running)

  def stop: F[Unit] = transitionTo(Status.Stopped) *> status.waitUntil(_ == Status.Init)

  def close: F[Unit] = transitionTo(Status.Closed) *> status.waitUntil(_ == Status.Init)

  private def transitionTo(newState: Status): F[Unit] =
    status
      .get
      .flatMap { currentState =>
        if (currentState.isTransitionAllowed(newState)) status.set(newState)
        else
          F.raiseError(
            new IllegalStateException(s"Invalid transition from $currentState to $newState")
          )
      }

}

object PartitionStream {

  sealed trait Status { self =>

    def isTransitionAllowed(status: Status): Boolean =
      (self, status) match {
        case (Status.Init, Status.Running)   => true
        case (Status.Running, Status.Paused) => true
        case (Status.Running, Status.Closed) => true
        case (Status.Paused, Status.Running) => true
        case (Status.Paused, Status.Stopped) => true
        case (_, Status.Init)                => true
        case (x, y) if x === y               => true
        case _                               => false
      }

    def isNotFinished: Boolean = self match {
      case Status.Stopped | Status.Closed => false
      case _                              => true
    }

  }

  object Status {

    case object Init    extends Status
    case object Running extends Status
    case object Paused  extends Status
    case object Stopped extends Status
    case object Closed  extends Status

    implicit val eq: Eq[Status] = Eq.fromUniversalEquals

  }

  def apply[F[_]: Async, K, V](
    partition: TopicPartition,
    requests: Queue[F, Request[F, K, V]]
  ): F[PartitionStream[F, K, V]] =
    SignallingRef
      .of[F, Status](Status.Init)
      .map(state => new PartitionStream(partition, requests.offer, state))

}
