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
import com.softwaremill.sttp.testing.SttpBackendStub
import com.softwaremill.sttp.{HttpURLConnectionBackend, Id, StatusCodes, SttpBackend}
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.filter.cache.{Cache, CacheMockAlwaysFalse, RedisCache}
import com.ubirch.filter.model.{FilterError, FilterErrorDeserializer, Rejection, RejectionDeserializer}
import com.ubirch.filter.util.Messages
import com.ubirch.kafka.MessageEnvelope
import com.ubirch.kafka.util.PortGiver
import com.ubirch.protocol.ProtocolMessage
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.json4s.JsonAST._
import org.scalatest.{BeforeAndAfter, MustMatchers, WordSpec}
import redis.embedded.RedisServer

import scala.concurrent.Future
import scala.language.postfixOps
import os.proc

import scala.util.Try


/**
 * This class provides all integration tests, except for those testing a missing redis connection on startup.
 * The Kafka config has to be inside each single test to enable parallel testing with different ports.
 */
class FilterServiceIntegrationTest extends WordSpec with EmbeddedKafka with EmbeddedRedis with MustMatchers with LazyLogging with BeforeAndAfter {

  implicit val seMsgEnv: Serializer[MessageEnvelope] = com.ubirch.kafka.EnvelopeSerializer
  implicit val deMsgEnv: Deserializer[MessageEnvelope] = com.ubirch.kafka.EnvelopeDeserializer
  implicit val deRej: Deserializer[Rejection] = RejectionDeserializer
  implicit val deError: Deserializer[FilterError] = FilterErrorDeserializer

  var redis: RedisServer = _

  /**
   * Method killing any embedded redis application, in case an earlier test was aborted without executing after.
   * Starting a new embedded redis server for testing purposes.
   */
  before {
    try {
      val embeddedRedisPid = getRedisPid(6379)
      proc("kill", "-9", embeddedRedisPid).call()
    } catch {
      case _: Throwable =>
    }
    redis = new RedisServer(6379)
    Thread.sleep(1000)
    redis.start()
  }

  private def getRedisPid(port: Int) = {
    proc("lsof", "-t", "-i", s":$port", "-s", "TCP:LISTEN").call().chunks.iterator
      .collect {
        case Left(s) => s
        case Right(s) => s
      }
      .map(x => new String(x.array)).map(_.trim.toInt).toList.head
  }

  /**
   * Called after all tests. Not working always unfortunately.
   */
  after {
    redis.stop()
  }

  /**
   * Method to start a filter service with a polling consumer.
   *
   * @param bootstrapServers The url configuration for each test and it's embedded kafka.
   */
  def startKafka(bootstrapServers: String): Unit = {

    val consumer: FilterService = new FilterService(RedisCache) {
      override val consumerBootstrapServers: String = bootstrapServers
      override val producerBootstrapServers: String = bootstrapServers
    }
    consumer.consumption.startPolling()
  }

  object KafkaForTest {
    var consumer: Option[FilterService] = None

    def startKafka(bootstrapServers: String): Unit = {
      consumer match {
        case None => consumer = Some(new FilterService(RedisCache) {
          override val consumerBootstrapServers: String = bootstrapServers
          override val producerBootstrapServers: String = bootstrapServers
        })
          //consumer.get.consumption.setForceExit(true)
          consumer.get.consumption.startPolling()
        case Some(value) =>
      }
    }

    def stopKafka = {
      consumer match {
        case Some(consum) =>
          //consum.consumption.setForceExit(true)
          logger.info("$$$ - shutting down kafka")
          consum.consumption.setForceExit(true)
          //consum.consumption.shutdown(1000, TimeUnit.MILLISECONDS)
        case None =>
      }
    }
  }

  /**
   * Method to generate a message envelope as expected by the filter service.
   *
   * @param payload Payload is a random value if not defined explicitly.
   * @return
   */
  private def generateMessageEnvelope(payload: Object = Base64.getEncoder.encode(UUID.randomUUID().toString.getBytes())): MessageEnvelope = {

    val pm = new ProtocolMessage(1, UUID.randomUUID(), 0, payload)
    pm.setSignature(org.bouncycastle.util.Strings.toByteArray("1111"))
    val ctxt = JObject("customerId" -> JString(UUID.randomUUID().toString))
    MessageEnvelope(pm, ctxt)
  }

  "FilterService" must {

    "forward message first time and detect reply attack with help of redis cache when send a second time" in {
      implicit val kafkaConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)
      val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort

      withRunningKafka {
        Thread.sleep(1000)
        val msgEnvelope = generateMessageEnvelope()
        //publish message first time
        publishToKafka(Messages.jsonTopic, msgEnvelope)
        Thread.sleep(1000)
        KafkaForTest.startKafka(bootstrapServers)
        Thread.sleep(1000)
        consumeFirstMessageFrom[MessageEnvelope](Messages.encodingTopic).ubirchPacket.getUUID mustBe
          msgEnvelope.ubirchPacket.getUUID

        //publish message second time (replay attack)
        publishToKafka(Messages.jsonTopic, msgEnvelope)

        val rejection = consumeFirstMessageFrom[Rejection](Messages.rejectionTopic)
        rejection.message mustBe Messages.foundInCacheMsg
        rejection.rejectionName mustBe Messages.replayAttackName
        Try(KafkaForTest.stopKafka)
      }
    }

    "also detect replay attack when cache is down" in {
      implicit val kafkaConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)
      val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort
      Thread.sleep(1000)

      withRunningKafka {
        Thread.sleep(1000)

        class FakeFilter(cache: Cache) extends FilterService(cache) {
          override val consumerBootstrapServers: String = bootstrapServers
          override val producerBootstrapServers: String = bootstrapServers

          override implicit val backend: SttpBackendStub[Id, Nothing] = SttpBackendStub.synchronous
            .whenRequestMatches(_ => true)
            .thenRespondOk()
        }
        val fakeFilter = new FakeFilter(new CacheMockAlwaysFalse)
        fakeFilter.consumption.startPolling()
        Thread.sleep(1000)

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
      Thread.sleep(1000)

      withRunningKafka {
        Thread.sleep(1000)

        implicit val serializer: Serializer[String] = new org.apache.kafka.common.serialization.StringSerializer()

        publishToKafka[String](Messages.jsonTopic, "unparseable example message")
        startKafka(bootstrapServers)
        Thread.sleep(1000)

        val message: FilterError = consumeFirstMessageFrom[FilterError](Messages.errorTopic)
        message.serviceName mustBe "filter-service"
        message.exceptionName mustBe "JsonParseException"
        message.message mustBe "unable to parse consumer record with key: null."
        assertThrows[TimeoutException] {
          consumeFirstMessageFrom[MessageEnvelope](Messages.encodingTopic)
        }
      }

    }

    "pause the consumption of new messages when there is an error sending messages" in {

      implicit val kafkaConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)
      val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort

      /**
       * just a cache variable that records what messages are being processed by the filter service
       */
      val cache = new Cache {
        var list: List[String] = List[String]()

        def get(hash: String): Boolean = {
          list = list :+ hash
          false
        }

        def set(hash: String): Boolean = {
          false
        }
      }

      /**
       * A fake filter service that always throws an exception, when the send() method is called.
       *
       * @param cache The cache is only used to record the messages being processed in this test.
       */
      class ExceptionFilterService(cache: Cache) extends FilterService(cache) {

        override val consumerBootstrapServers: String = bootstrapServers
        override val producerBootstrapServers: String = bootstrapServers

        override def send(topic: String, value: Array[Byte]): Future[RecordMetadata] = {
          Future {
            throw new Exception("test exception")
          }
        }
      }

      withRunningKafka {
        val fakeFilterService = new ExceptionFilterService(cache)
        fakeFilterService.consumption.startPolling()

        val msgEnvelope1 = generateMessageEnvelope()
        val msgEnvelope2 = generateMessageEnvelope()

        publishToKafka(Messages.jsonTopic, msgEnvelope1)
        Thread.sleep(5000)
        publishToKafka(Messages.jsonTopic, msgEnvelope2)

        Thread.sleep(5000)
        println(cache.list)

        /**
         * the first two messages should be msgEnvelope1
         */
        assert(cache.list.head == cache.list(1))

        /**
         * the last two messages should be msgEnvelope1 and msgEnvelope2 as the consumer repeats consuming the
         * not yet committed messages.
         */
        assert(cache.list.last != cache.list(cache.list.size - 2))

      }
    }

    "forward all messages when activeState equals false" in {
      implicit val kafkaConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)
      val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort

      withRunningKafka {

        class FakeFilter(cache: Cache) extends FilterService(cache) {
          override val consumerBootstrapServers: String = bootstrapServers
          override val producerBootstrapServers: String = bootstrapServers
          override val filterStateActive = false
        }
        val fakeFilter = new FakeFilter(new CacheMockAlwaysFalse)
        fakeFilter.consumption.startPolling()

        val msgEnvelope = generateMessageEnvelope()
        publishToKafka(Messages.jsonTopic, msgEnvelope)

        val forwardedMessage = consumeFirstMessageFrom[MessageEnvelope](Messages.encodingTopic)
        forwardedMessage.ubirchPacket.getUUID mustEqual msgEnvelope.ubirchPacket.getUUID
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

}
