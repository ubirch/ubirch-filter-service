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

import java.util.{Base64, UUID}

import com.github.sebruck.EmbeddedRedis
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.filter.cache.{CacheMockAlwaysFalse, RedisCache}
import com.ubirch.filter.model.{Rejection, RejectionDeserializer}
import com.ubirch.filter.util.Messages
import com.ubirch.kafka.MessageEnvelope
import com.ubirch.protocol.ProtocolMessage
import com.ubirch.util.PortGiver
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.json4s.JsonAST.{JObject, JString}
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpec}
import redis.embedded.RedisServer

class NoCacheScenarioTest extends WordSpec with EmbeddedKafka with EmbeddedRedis with MustMatchers with LazyLogging with BeforeAndAfterAll {

  var redis: RedisServer = _
  implicit val seMsgEnv: Serializer[MessageEnvelope] = com.ubirch.kafka.EnvelopeSerializer
  implicit val deRej: Deserializer[Rejection] = RejectionDeserializer

  override def beforeAll: Unit = {
    redis = new RedisServer(6379)
    redis.start()
    implicit val kafkaConfig: EmbeddedKafkaConfig =
      EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)

    val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort
    val consumer: FilterService = new FilterService(new CacheMockAlwaysFalse) {
      override val consumerBootstrapServers: String = bootstrapServers
      override val producerBootstrapServers: String = bootstrapServers
    }
    consumer.consumption.startPolling()
  }

  override def afterAll: Unit = {
    redis.stop()
  }


  "FilterService" must {

    "detect replay attack of message whose payload has been processed already by the verification lookup" in {

      withRunningKafka {
        val specialConsumerTopic = "json.to.sign.special"

        val filter = new FilterService(new CacheMockAlwaysFalse) {
          override val consumerTopics: Set[String] = Set(specialConsumerTopic)
        }
        val payload =
          "v1jjADSpJFPl7vTg8gsiui2aTOCL1v4nYrmiTePvcxNBt1x/+JoApHOLa4rjGGEz72PusvGXbF9t6qe8Kbck/w=="
        val msgEnvelope = generateMessageEnvelope(payload)

        //publish message of which the payload already has been processed once
        publishToKafka(specialConsumerTopic, msgEnvelope)

        val rejection = consumeFirstMessageFrom[Rejection](Messages.rejectionTopic)
        rejection.id mustBe msgEnvelope.ubirchPacket.getUUID
        rejection.message mustBe Messages.foundInVerificationMsg
        rejection.rejectionName mustBe Messages.replayAttackName

        Thread.sleep(5000)
      }
    }
  }

  private def generateMessageEnvelope(payload: Object = Base64.getEncoder.encode(UUID.randomUUID().toString.getBytes())): MessageEnvelope = {

    val pm = new ProtocolMessage(1, UUID.randomUUID(), 0, payload)
    pm.setSignature(org.bouncycastle.util.Strings.toByteArray("1111"))
    val ctxt = JObject("customerId" -> JString(UUID.randomUUID().toString))
    MessageEnvelope(pm, ctxt)
  }

}
