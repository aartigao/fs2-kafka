/*
 * Copyright 2018-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.experimental

import cats.*
import cats.effect.*
import cats.effect.kernel.Resource.ExitCase
import cats.syntax.all.*
import fs2.*
import fs2.concurrent.*
import fs2.kafka.{CommittableConsumerRecord, ConsumerSettings}
import fs2.kafka.instances.*
import fs2.kafka.internal.experimental.PartitionStream.{FetchCallback, Status}

import org.apache.kafka.common.TopicPartition

class PartitionStream[F[_], K, V](
  settings: ConsumerSettings[F, K, V],
  storeFetch: FetchCallback[F, K, V] => F[Unit],
  onFinish: Status => F[Unit],
  status: SignallingRef[F, Status]
)(implicit F: Async[F]) {

  private val pauseSignal     = status.map(_ == Status.Paused)
  private val interruptSignal = status.map(_ == Status.Stopping)

  def create: Stream[F, CommittableConsumerRecord[F, K, V]] =
    Stream.exec(transitionTo(Status.Running)) ++
      status
        .continuous
        .takeWhile(_.isNotFinish)
        .evalMap { _ =>
          F.async[Chunk[CommittableConsumerRecord[F, K, V]]] { cb =>
            storeFetch(cb).as(Some(F.unit))
          }
        }
        .debugChunks()
        .prefetchN(settings.maxPrefetchBatches - 1)
        .unchunks
        .pauseWhen(pauseSignal)
        .interruptWhen(interruptSignal)
        .onFinalizeCase(exit => onExitTransition(exit) >>= onFinish)

  private[this] def onExitTransition(exit: ExitCase): F[Status] = exit match {
    case ExitCase.Succeeded  => transitionTo(Status.Completed).as(Status.Completed)
    case ExitCase.Canceled   => transitionTo(Status.Stopped).as(Status.Stopped)
    case _: ExitCase.Errored => transitionTo(Status.Failed).as(Status.Failed)
  }

  def isInit: F[Boolean] = status.get.map(_ == Status.Init)

  def pause: F[Unit] = transitionTo(Status.Paused)

  def isPaused: F[Boolean] = status.get.map(_ == Status.Paused)

  def resume: F[Unit] = transitionTo(Status.Running)

  def stop: F[Status] = transitionToFinish(Status.Stopping).flatten

  def close: F[F[Status]] = transitionToFinish(Status.Closing)

  private[this] def transitionToFinish(newStatus: Status): F[F[Status]] =
    transitionTo(newStatus).as(status.waitUntil(_.isFinished) >> status.get)

  private[this] def transitionTo(newStatus: Status): F[Unit] =
    status
      .get
      .flatTap(status => F.pure(println(s"$this -> From $status to $newStatus")))
      .flatMap { currentStatus =>
        if (currentStatus.isTransitionAllowed(newStatus)) status.set(newStatus)
        else
          F.raiseError(
            new IllegalStateException(s"Invalid transition from $currentStatus to $newStatus")
          )
      }

}

object PartitionStream {

  type FetchCallback[F[_], K, V] =
    Either[Throwable, Chunk[CommittableConsumerRecord[F, K, V]]] => Unit

  sealed trait Status { self =>

    def isTransitionAllowed(status: Status): Boolean =
      (self, status) match {
        case (Status.Init, Status.Running)    => true
        case (Status.Running, Status.Paused)  => true
        case (Status.Running, Status.Closing) => true
        case (Status.Paused, Status.Running)  => true
        case (Status.Paused, Status.Stopping) => true
        case (_, Status.Completed)            => true
        case (_, Status.Stopped)              => true
        case (_, Status.Failed)               => true
        case (x, y) if x === y                => true
        case _                                => false
      }

    def isFinished: Boolean = self match {
      case Status.Completed | Status.Failed | Status.Stopped => true
      case _                                                 => false
    }

    def isNotFinish: Boolean = self match {
      case Status.Stopping | Status.Closing => false
      case _                                => !isFinished
    }

  }

  object Status {

    case object Init      extends Status
    case object Running   extends Status
    case object Paused    extends Status
    case object Closing   extends Status
    case object Stopping  extends Status
    case object Completed extends Status
    case object Stopped   extends Status
    case object Failed    extends Status

    implicit val eq: Eq[Status]     = Eq.fromUniversalEquals
    implicit val show: Show[Status] = Show.fromToString

  }

  def apply[F[_]: Async, K, V](
    settings: ConsumerSettings[F, K, V],
    storeFetch: FetchCallback[F, K, V] => F[Unit],
    onFinish: Status => F[Unit]
  ): F[PartitionStream[F, K, V]] =
    SignallingRef
      .of[F, Status](Status.Init)
      .map(state => new PartitionStream(settings, storeFetch, onFinish, state))

}
