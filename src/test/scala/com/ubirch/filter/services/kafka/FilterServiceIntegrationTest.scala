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

package com.ubirch.filter.services.kafka

import java.util.concurrent.TimeoutException
import java.util.{Base64, UUID}

import com.github.sebruck.EmbeddedRedis
import com.google.inject.binder.ScopedBindingBuilder
import com.softwaremill.sttp.testing.SttpBackendStub
import com.softwaremill.sttp.{HttpURLConnectionBackend, Id, StatusCodes, SttpBackend}
import com.typesafe.config.{Config, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.filter.cache.{Cache, CacheMockAlwaysFalse, RedisCache}
import com.ubirch.filter.model.{FilterError, FilterErrorDeserializer, Rejection, RejectionDeserializer}
import com.ubirch.filter.util.Messages
import com.ubirch.filter.{Binder, InjectorHelper}
import com.ubirch.filter.services.config.ConfigProvider
import com.ubirch.filter.ConfPaths.{ConsumerConfPaths, ProducerConfPaths}
import com.ubirch.filter.model.eventlog.CassandraFinder
import com.ubirch.filter.services.Lifecycle
import com.ubirch.kafka.MessageEnvelope
import com.ubirch.kafka.util.PortGiver
import com.ubirch.protocol.ProtocolMessage
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.json4s.JsonAST._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach, MustMatchers, WordSpec}
import redis.embedded.RedisServer
import io.prometheus.client.CollectorRegistry
import javax.inject.{Inject, Singleton}

import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import os.proc

import scala.util.Try


/**
 * This class provides all integration tests, except for those testing a missing redis connection on startup.
 * The Kafka config has to be inside each single test to enable parallel testing with different ports.
 */
class FilterServiceIntegrationTest extends WordSpec with EmbeddedKafka with EmbeddedRedis with MustMatchers with LazyLogging with BeforeAndAfter with BeforeAndAfterEach {

  implicit val seMsgEnv: Serializer[MessageEnvelope] = com.ubirch.kafka.EnvelopeSerializer
  implicit val deMsgEnv: Deserializer[MessageEnvelope] = com.ubirch.kafka.EnvelopeDeserializer
  implicit val deRej: Deserializer[Rejection] = RejectionDeserializer
  implicit val deError: Deserializer[FilterError] = FilterErrorDeserializer

  var redis: RedisServer = _

  /**
  * Overwrite default bootstrap server and topic values of the kafka consumer and producers
    */
  def customTestConfigProvider(bootstrapServers: String, consumerTopic: String): ConfigProvider = new ConfigProvider {
    override def conf: Config = super.conf.withValue(
      ConsumerConfPaths.BOOTSTRAP_SERVERS,
      ConfigValueFactory.fromAnyRef(bootstrapServers)
    ).withValue(
      ProducerConfPaths.BOOTSTRAP_SERVERS,
      ConfigValueFactory.fromAnyRef(bootstrapServers)
    ).withValue(
      ConsumerConfPaths.TOPICS,
      ConfigValueFactory.fromAnyRef(consumerTopic)
    )
  }


  def FakeSimpleInjector(bootstrapServers: String, consumerTopic: String): InjectorHelper = new InjectorHelper(List(new Binder {
    override def Config: ScopedBindingBuilder = bind(classOf[Config]).toProvider(customTestConfigProvider(bootstrapServers, consumerTopic))
  })) {}


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
    logger.info("Shutting off test services")
    logger.info("Shutting off test services - REDIS")
    redis.stop()
    logger.info("Shutting off test services - EMBEDDED KAFKA")
    EmbeddedKafka.stop()
    EmbeddedKafka.stopZooKeeper()
    Thread.sleep(10000)
  }

  override protected def beforeEach() = {
    CollectorRegistry.defaultRegistry.clear()
  }

  override protected def afterEach(): Unit = {
    while(EmbeddedKafka.isRunning) {
      logger.info("Kafka still running...")
      Thread.sleep(1000)
      EmbeddedKafka.stop()
    }
    logger.info(s"Embedded kafka status: isRunning = ${EmbeddedKafka.isRunning}")
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
        val Injector = FakeSimpleInjector(bootstrapServers, Messages.jsonTopic)
        val consumer = Injector.get[DefaultFilterService]
        consumer.consumption.startPolling()
        Thread.sleep(1000)
        consumeFirstMessageFrom[MessageEnvelope](Messages.encodingTopic).ubirchPacket.getUUID mustBe
          msgEnvelope.ubirchPacket.getUUID

        //publish message second time (replay attack)
        publishToKafka(Messages.jsonTopic, msgEnvelope)

        val rejection = consumeFirstMessageFrom[Rejection](Messages.rejectionTopic)
        rejection.message mustBe Messages.foundInCacheMsg
        rejection.rejectionName mustBe Messages.replayAttackName
      }
    }

    "also detect replay attack when cache is down" in {
      implicit val kafkaConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)
      val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort

      EmbeddedKafka.start()
      Thread.sleep(1000)

      def FakeInjector(bootstrapServers: String, consumerTopic: String): InjectorHelper = new InjectorHelper(List(new Binder {
        override def Config: ScopedBindingBuilder = bind(classOf[Config]).toProvider(customTestConfigProvider(bootstrapServers, consumerTopic))
        override def FilterService: ScopedBindingBuilder = bind(classOf[AbstractFilterService]).to(classOf[FakeFilterReplayAttack])
      })) {}

      val Injector = FakeInjector(bootstrapServers, Messages.jsonTopic)

      val fakeFilter = Injector.get[AbstractFilterService]
      fakeFilter.consumption.startPolling()
      Thread.sleep(1000)

      val msgEnvelope = generateMessageEnvelope()
      publishToKafka(Messages.jsonTopic, msgEnvelope)

      val rejection = consumeFirstMessageFrom[Rejection](Messages.rejectionTopic)
      rejection.message mustBe Messages.foundInVerificationMsg
      rejection.rejectionName mustBe Messages.replayAttackName

      EmbeddedKafka.stop()
    }

    "not forward unparseable messages" in {
      implicit val kafkaConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)
      val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort
      Thread.sleep(1000)

      EmbeddedKafka.start()
      Thread.sleep(1000)

      implicit val serializer: Serializer[String] = new org.apache.kafka.common.serialization.StringSerializer()

      publishToKafka[String](Messages.jsonTopic, "unparseable example message")

      val Injector = FakeSimpleInjector(bootstrapServers, Messages.jsonTopic)

      val consumer: DefaultFilterService = Injector.get[DefaultFilterService]
      consumer.consumption.startPolling()
      Thread.sleep(1000)

      val message: FilterError = consumeFirstMessageFrom[FilterError](Messages.errorTopic)
      message.serviceName mustBe "filter-service"
      message.exceptionName mustBe "JsonParseException"
      message.message mustBe "unable to parse consumer record with key: null."
      assertThrows[TimeoutException] {
        consumeFirstMessageFrom[MessageEnvelope](Messages.encodingTopic)
      }
      EmbeddedKafka.stop()

    }

    "pause the consumption of new messages when there is an error sending messages" in {

      implicit val kafkaConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)
      val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort

      EmbeddedKafka.start()

      def FakeInjectorCustomCache(configProvider: ConfigProvider): InjectorHelper = new InjectorHelper(List(new Binder {
        override def Config: ScopedBindingBuilder = bind(classOf[Config]).toProvider(configProvider)
        override def FilterService: ScopedBindingBuilder = bind(classOf[AbstractFilterService]).to(classOf[ExceptionFilterServ])
        override def Cache: ScopedBindingBuilder = bind(classOf[Cache]).to(classOf[CustomCache])
      })) {}

      val Injector = FakeInjectorCustomCache(customTestConfigProvider(bootstrapServers, Messages.jsonTopic))
      val fakeFilterService = Injector.get[AbstractFilterService]

      val cache = Injector.get[Cache].asInstanceOf[CustomCache]
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

      EmbeddedKafka.stop()
    }

    "forward all messages when activeState equals false" in {
      implicit val kafkaConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)
      val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort

      EmbeddedKafka.start()

      def FakeInjector(bootstrapServers: String, consumerTopic: String): InjectorHelper = new InjectorHelper(List(new Binder {
        override def Config: ScopedBindingBuilder = bind(classOf[Config]).toProvider(customTestConfigProvider(bootstrapServers, consumerTopic))
        override def FilterService: ScopedBindingBuilder = bind(classOf[AbstractFilterService]).to(classOf[FakeFilterDoesNothing])
      })) {}

      val Injector = FakeInjector(bootstrapServers, Messages.jsonTopic)

      val fakeFilter = Injector.get[FakeFilterDoesNothing]
      fakeFilter.consumption.startPolling()

      val msgEnvelope = generateMessageEnvelope()
      publishToKafka(Messages.jsonTopic, msgEnvelope)

      val forwardedMessage = consumeFirstMessageFrom[MessageEnvelope](Messages.encodingTopic)
      forwardedMessage.ubirchPacket.getUUID mustEqual msgEnvelope.ubirchPacket.getUUID
      EmbeddedKafka.stop()
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
      val Injector = FakeSimpleInjector(ConsumerConfPaths.BOOTSTRAP_SERVERS, ConsumerConfPaths.TOPICS)
      val filterConsumer = Injector.get[DefaultFilterService]
      val result = filterConsumer.makeVerificationLookup(data)
      //assert(result.code == StatusCodes.NotFound)
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

// Some classes have to be defnied outside of the functions that uses them, as Guice does not support injecting into inner classes

@Singleton
class FakeFilterReplayAttack @Inject()(cache: Cache, cassandraFinder: CassandraFinder, config: Config, lifecycle: Lifecycle)(implicit val ec: ExecutionContext) extends AbstractFilterService(cache, cassandraFinder, config, lifecycle) {
  override implicit val backend: SttpBackendStub[Id, Nothing] = SttpBackendStub.synchronous
    .whenRequestMatches(_ => true)
    .thenRespondOk()
}

/**
  * A fake filter service that always throws an exception, when the send() method is called.
  *
  * @param cache The cache is only used to record the messages being processed in this test.
  */
@Singleton
class ExceptionFilterServ @Inject()(cache: Cache, cassandraFinder: CassandraFinder, config: Config, lifecycle: Lifecycle)(override implicit val ec: ExecutionContext) extends AbstractFilterService(cache: Cache, cassandraFinder, config: Config, lifecycle: Lifecycle) {
  override def send(topic: String, value: Array[Byte]): Future[RecordMetadata] = {
    Future {
      throw new Exception("test exception")
    }
  }
}

/**
  * just a cache variable that records what messages are being processed by the filter service
  */
@Singleton
class CustomCache extends Cache {
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
*   Fake filter that has the filterStateActive set to true. Should always forward the messages
  * @param cache The cache used to check if a message has already been received before.
  */
@Singleton
class FakeFilterDoesNothing @Inject()(cache: Cache, cassandraFinder: CassandraFinder, config: Config, lifecycle: Lifecycle)(override implicit val ec: ExecutionContext) extends DefaultFilterService(cache, cassandraFinder, config, lifecycle) {
  override val filterStateActive = false
}
