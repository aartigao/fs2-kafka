/*
 * Copyright 2018-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.internal.experimental

import scala.concurrent.duration.*

import cats.data.NonEmptyList
import cats.effect.unsafe.implicits.global
import cats.effect.IO
import fs2.kafka.{BaseKafkaSpec, CommittableConsumerRecord}
import fs2.Stream

final class KafkaConsumerSpec extends BaseKafkaSpec {

  type Consumer = KafkaConsumer[IO, String, String]

  type ConsumerStream = Stream[IO, CommittableConsumerRecord[IO, String, String]]

  describe("KafkaConsumer#stream") {
    it("should consume all records with subscribe") {
      withTopic { topic =>
        createCustomTopic(topic, partitions = 3)

        val produced = (0 until 5).map(n => s"key-$n" -> s"value->$n")
        publishToKafka(topic, produced)

        val consumed =
          (for {
            consumer    <- KafkaConsumer.stream(consumerSettings[IO])
            _           <- Stream.eval(consumer.subscribe(NonEmptyList.one(topic)))
            committable <- consumer.records
          } yield committable.record.key -> committable.record.value)
//            .interruptAfter(5.seconds)
            .take(5)
            .compile
            .toVector
            .unsafeRunSync()

        consumed should contain theSameElementsAs produced
      }
    }
  }

}
