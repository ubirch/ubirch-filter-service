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

class ConsumerTest

import java.util.concurrent.TimeoutException
import java.util.{Base64, UUID}

import com.github.sebruck.EmbeddedRedis
import com.softwaremill.sttp.{HttpURLConnectionBackend, Id, StatusCodes, SttpBackend}
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.filter.cache.RedisCache
import com.ubirch.filter.model.{Rejection, RejectionDeserializer}
import com.ubirch.filter.util.Messages
import com.ubirch.kafka.MessageEnvelope
import com.ubirch.protocol.ProtocolMessage
import com.ubirch.util.PortGiver
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.json4s.JsonAST._
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpec}
import redis.embedded.RedisServer


class FilterServiceIntegrationTest extends WordSpec with EmbeddedKafka with EmbeddedRedis with MustMatchers with LazyLogging with BeforeAndAfterAll {

  implicit val seMsgEnv: Serializer[MessageEnvelope] = com.ubirch.kafka.EnvelopeSerializer
  implicit val deMsgEnv: Deserializer[MessageEnvelope] = com.ubirch.kafka.EnvelopeDeserializer
  implicit val deRej: Deserializer[Rejection] = RejectionDeserializer

  var redis: RedisServer = _

  override def beforeAll: Unit = {

    redis = new RedisServer(6379)
    redis.start()
    implicit val kafkaConfig: EmbeddedKafkaConfig =
      EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)

    val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort
    val consumer: FilterService = new FilterService(RedisCache) {
      override val consumerBootstrapServers: String = bootstrapServers
      override val producerBootstrapServers: String = bootstrapServers
    }
    consumer.consumption.startPolling()
  }

  override def afterAll: Unit = {
    redis.stop()
  }


  "FilterService" must {

    "forward message first time and detect reply attack with help of redis cache when send a second time" in {


      withRunningKafka {

        val messageEnvelopeTopic = Messages.jsonTopic
        val msgEnvelope = generateMessageEnvelope()

        //publish message first time
        publishToKafka(messageEnvelopeTopic, msgEnvelope)
        consumeFirstMessageFrom[MessageEnvelope](Messages.encodingTopic).ubirchPacket.getUUID mustBe
          msgEnvelope.ubirchPacket.getUUID

        //publish message second time (replay attack)
        publishToKafka(messageEnvelopeTopic, msgEnvelope)

        val rejection = consumeFirstMessageFrom[Rejection](Messages.rejectionTopic)
        rejection.id mustBe msgEnvelope.ubirchPacket.getUUID
        rejection.message mustBe Messages.foundInCacheMsg
        rejection.rejectionName mustBe Messages.replayAttackName

        Thread.sleep(7000)
      }
    }

    /*
    //Todo: Shouldn't this eventually send something to the errormessage queue?
    "not forward unparseable messages" in {

      withRunningKafka {

        val messageEnvelopeTopic = "json.to.sign"
        implicit val serializer: Serializer[String] = new org.apache.kafka.common.serialization.StringSerializer()

        publishToKafka[String](messageEnvelopeTopic, "unpearsable example message")

        assertThrows[TimeoutException] {
          consumeFirstMessageFrom[MessageEnvelope](Messages.encodingTopic)
        }
        Thread.sleep(7000)
      }
    }*/

  }


  "Verification Lookup" must {

    implicit val backend: SttpBackend[Id, Nothing] = HttpURLConnectionBackend()

    "return 200 when hash already has been processed once" in {
      val payloadEncoded = "v1jjADSpJFPl7vTg8gsiui2aTOCL1v4nYrmiTePvcxNBt1x/+JoApHOLa4rjGGEz72PusvGXbF9t6qe8Kbck/w=="
      val filterConsumer = new FilterService(RedisCache)
      val result = filterConsumer.makeVerificationLookup(payloadEncoded)
      assert(result.code == StatusCodes.Ok)
    }

    "return 404 if hash hasn't been processed yet." in {
      val payload = UUID.randomUUID().toString
      val filterConsumer = new FilterService(RedisCache)
      val result = filterConsumer.makeVerificationLookup(payload)
      assert(result.code == StatusCodes.NotFound)
    }

  }

  "Payload encoding" must {

    "be encoded when retrieved by asText() and decoded when retrieved by binaryValue()" in {
      val payload =
        "v1jjADSpJFPl7vTg8gsiui2aTOCL1v4nYrmiTePvcxNBt1x/+JoApHOLa4rjGGEz72PusvGXbF9t6qe8Kbck/w=="
      val decodedPayload = Base64.getDecoder.decode(payload)
      val envelope = generateMessageEnvelope(payload)
      assert(envelope.ubirchPacket.getPayload.asText() == payload)
      assert(envelope.ubirchPacket.getPayload.binaryValue() sameElements decodedPayload)
    }
  }

  private def generateMessageEnvelope(payload: Object = Base64.getEncoder.encode(UUID.randomUUID().toString.getBytes())): MessageEnvelope = {

    val pm = new ProtocolMessage(1, UUID.randomUUID(), 0, payload)
    pm.setSignature(org.bouncycastle.util.Strings.toByteArray("1111"))
    val ctxt = JObject("customerId" -> JString(UUID.randomUUID().toString))
    MessageEnvelope(pm, ctxt)
  }


}
