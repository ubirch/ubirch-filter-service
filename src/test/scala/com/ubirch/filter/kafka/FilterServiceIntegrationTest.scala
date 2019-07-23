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
import com.softwaremill.sttp.testing.SttpBackendStub
import com.softwaremill.sttp.{HttpURLConnectionBackend, Id, StatusCodes, SttpBackend}
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.filter.cache.{Cache, CacheMockAlwaysFalse, RedisCache}
import com.ubirch.filter.model.{FilterError, FilterErrorDeserializer, Rejection, RejectionDeserializer}
import com.ubirch.filter.util.Messages
import com.ubirch.kafka.MessageEnvelope
import com.ubirch.protocol.ProtocolMessage
import com.ubirch.util.PortGiver
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.json4s.JsonAST._
import org.scalatest.{BeforeAndAfter, MustMatchers, WordSpec}
import redis.embedded.RedisServer

class FilterServiceIntegrationTest extends WordSpec with EmbeddedKafka with EmbeddedRedis with MustMatchers with LazyLogging with BeforeAndAfter {

  implicit val seMsgEnv: Serializer[MessageEnvelope] = com.ubirch.kafka.EnvelopeSerializer
  implicit val deMsgEnv: Deserializer[MessageEnvelope] = com.ubirch.kafka.EnvelopeDeserializer
  implicit val deRej: Deserializer[Rejection] = RejectionDeserializer
  implicit val deError: Deserializer[FilterError] = FilterErrorDeserializer

  var redis: RedisServer = _

  before {
    redis = new RedisServer(6379)
    redis.start()
  }

  after {
    redis.stop()
  }

  def startKafka(bootstrapServers: String): Unit = {

    val consumer: FilterService = new FilterService(RedisCache) {
      override val consumerBootstrapServers: String = bootstrapServers
      override val producerBootstrapServers: String = bootstrapServers
    }
    consumer.consumption.startPolling()
  }

  "FilterService" must {

    "forward message first time and detect reply attack with help of redis cache when send a second time" in {
      implicit val kafkaConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)
      val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort

      withRunningKafka {

        val msgEnvelope = generateMessageEnvelope()
        //publish message first time
        publishToKafka(Messages.jsonTopic, msgEnvelope)
        startKafka(bootstrapServers)
        consumeFirstMessageFrom[MessageEnvelope](Messages.encodingTopic).ubirchPacket.getUUID mustBe
          msgEnvelope.ubirchPacket.getUUID

        //publish message second time (replay attack)
        publishToKafka(Messages.jsonTopic, msgEnvelope)

        val rejection = consumeFirstMessageFrom[Rejection](Messages.rejectionTopic)
        rejection.message mustBe Messages.foundInCacheMsg
        rejection.rejectionName mustBe Messages.replayAttackName
      }
    }

    "must also detect replay attack when cache is down" in {
      implicit val kafkaConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)
      val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort

      withRunningKafka {

        class FakeFilter(cache: Cache) extends FilterService(cache) {
          override val consumerBootstrapServers: String = bootstrapServers
          override val producerBootstrapServers: String = bootstrapServers

          override implicit val backend: SttpBackendStub[Id, Nothing] = SttpBackendStub.synchronous
            .whenRequestMatches(_ => true)
            .thenRespondOk()
        }
        val fakeFilter = new FakeFilter(new CacheMockAlwaysFalse)
        fakeFilter.consumption.startPolling()

        val msgEnvelope = generateMessageEnvelope()
        publishToKafka(Messages.jsonTopic, msgEnvelope)

        val rejection = consumeFirstMessageFrom[Rejection](Messages.rejectionTopic)
        rejection.message mustBe Messages.foundInVerificationMsg
        rejection.rejectionName mustBe Messages.replayAttackName
      }
    }

    "not forward unparseable messages" in {
      implicit val kafkaConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)
      val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort

      withRunningKafka {

        implicit val serializer: Serializer[String] = new org.apache.kafka.common.serialization.StringSerializer()

        publishToKafka[String](Messages.jsonTopic, "unparseable example message")
        startKafka(bootstrapServers)
        val message: FilterError = consumeFirstMessageFrom[FilterError](Messages.errorTopic)
        message.serviceName mustBe "filter-service"
        message.exceptionName mustBe "JsonParseException"
        message.message mustBe "unable to parse consumer record with key: null."
        assertThrows[TimeoutException] {
          consumeFirstMessageFrom[MessageEnvelope](Messages.encodingTopic)
        }
      }
    }
  }

  "Verification Lookup" must {

    implicit val backend: SttpBackend[Id, Nothing] = HttpURLConnectionBackend()

    //Todo: Remove? As I always need a hash, which really has been used in Dev. Mikolaj even said, the processing should have been aborted, so it returns true.
    //Todo continue: But as I understood it, as soon as it has been processed anytime it should return true.?
    //    "return 200 when hash already has been processed once" in {
    //      val payloadEncoded = "v1jjADSpJFPl7vTg8gsiui2aTOCL1v4nYrmiTePvcxNBt1x/+JoApHOLa4rjGGEz72PusvGXbF9t6qe8Kbck/w=="
    //      val filterConsumer = new FilterService(RedisCache)
    //      val result = filterConsumer.makeVerificationLookup(payloadEncoded)
    //      assert(result.code == StatusCodes.Ok)
    //    }

    "return NotFound if hash hasn't been processed yet." in {
      val cr = new ConsumerRecord[String, Array[Byte]]("topic", 1, 1, "key", "Teststring".getBytes)
      val payload = UUID.randomUUID().toString
      val data = ProcessingData(cr, payload)
      val filterConsumer = new FilterService(RedisCache)
      val result = filterConsumer.makeVerificationLookup(data)
      assert(result.code == StatusCodes.NotFound)
    }
  }

  "Payload encoding" must {

    "be encoded when retrieved with asText() and decoded when retrieved with binaryValue()" in {
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
