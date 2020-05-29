/*
 * Copyright (c) 2019 ubirch GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ubirch.filter.kafka

import java.util.concurrent.TimeoutException
import java.util.{Base64, UUID}

import com.github.sebruck.EmbeddedRedis
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.filter.cache.RedisCache
import com.ubirch.filter.model.{FilterError, FilterErrorDeserializer, Rejection, RejectionDeserializer}
import com.ubirch.filter.util.Messages
import com.ubirch.kafka.MessageEnvelope
import com.ubirch.protocol.ProtocolMessage
import com.ubirch.kafka.util.PortGiver
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.json4s.JsonAST.{JObject, JString}
import org.scalatest.{BeforeAndAfter, MustMatchers, WordSpec}
import redis.embedded.RedisServer

import scala.language.postfixOps
import scala.sys.process._

class FilterServiceIntegrationTestWithoutCache extends WordSpec with EmbeddedKafka with EmbeddedRedis with MustMatchers with LazyLogging with BeforeAndAfter {

  implicit val seMsgEnv: Serializer[MessageEnvelope] = com.ubirch.kafka.EnvelopeSerializer
  implicit val deMsgEnv: Deserializer[MessageEnvelope] = com.ubirch.kafka.EnvelopeDeserializer
  implicit val deRej: Deserializer[Rejection] = RejectionDeserializer
  implicit val deError: Deserializer[FilterError] = FilterErrorDeserializer

  var redis: RedisServer = _

  def startKafka(bootstrapServers: String): Unit = {

    val consumer: FilterService = new FilterService(RedisCache) {
      override val consumerBootstrapServers: String = bootstrapServers
      override val producerBootstrapServers: String = bootstrapServers
    }
    consumer.consumption.startPolling()
  }

  "Missing redis connection" must {

    "should cause error messages" in {
      implicit val kafkaConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)
      val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort

      withRunningKafka {

        try {
          "fuser -k 6379/tcp" !
        } catch {
          case _: Throwable =>
        }

        val msgEnvelope = generateMessageEnvelope()

        startKafka(bootstrapServers)

        //publish message first time
        publishToKafka(Messages.jsonTopic, msgEnvelope)

        consumeFirstMessageFrom[MessageEnvelope](Messages.encodingTopic).ubirchPacket.getUUID mustBe
          msgEnvelope.ubirchPacket.getUUID

        val cacheError1 = consumeFirstMessageFrom[FilterError](Messages.errorTopic)
        cacheError1.exceptionName mustBe "NoCacheConnectionException"

      }
    }

    "not change default behaviour" in {
      implicit val kafkaConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)
      val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort

      withRunningKafka {

        try {
          "fuser -k 6379/tcp" !
        } catch {
          case _: Throwable =>
        }

        val msgEnvelope = generateMessageEnvelope()
        //publish message first time
        publishToKafka(Messages.jsonTopic, msgEnvelope)
        startKafka(bootstrapServers)
        val cacheError1 = consumeFirstMessageFrom[FilterError](Messages.errorTopic)
        cacheError1.exceptionName mustBe "NoCacheConnectionException"
        consumeFirstMessageFrom[MessageEnvelope](Messages.encodingTopic).ubirchPacket.getUUID mustBe
          msgEnvelope.ubirchPacket.getUUID

        //publish message second time (replay attack)
        publishToKafka(Messages.jsonTopic, msgEnvelope)

        val cacheError2 = consumeFirstMessageFrom[FilterError](Messages.errorTopic)
        cacheError2.exceptionName mustBe "NoCacheConnectionException"

      }
    }

    "get fixed, when redis server starts delayed" in {
      implicit val kafkaConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)
      val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort

      withRunningKafka {

        try {
          "fuser -k 6379/tcp" !
        } catch {
          case _: Throwable =>
        }

        val msgEnvelope1 = generateMessageEnvelope()

        //publish first message
        publishToKafka(Messages.jsonTopic, msgEnvelope1)
        startKafka(bootstrapServers)
        consumeFirstMessageFrom[MessageEnvelope](Messages.encodingTopic).ubirchPacket.getUUID mustBe
          msgEnvelope1.ubirchPacket.getUUID
        val cacheError1 = consumeFirstMessageFrom[FilterError](Messages.errorTopic)
        cacheError1.exceptionName mustBe "NoCacheConnectionException"
        redis = new RedisServer(6379)
        redis.start()
        Thread.sleep(8000)

        //publish second message time
        val msgEnvelope2 = generateMessageEnvelope()
        publishToKafka(Messages.jsonTopic, msgEnvelope2)

        assertThrows[TimeoutException] {
          consumeNumberMessagesFrom[FilterError](Messages.errorTopic, 2)
        }
        redis.stop()
      }
    }
  }

  private def generateMessageEnvelope(payload: Object = Base64.getEncoder.encode(UUID.randomUUID().toString.getBytes())): MessageEnvelope = {

    val pm = new ProtocolMessage(1, UUID.randomUUID(), 0, payload)
    pm.setSignature("1111".getBytes())
    val ctxt = JObject("customerId" -> JString(UUID.randomUUID().toString))
    MessageEnvelope(pm, ctxt)
  }

}
