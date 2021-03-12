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

import com.github.sebruck.EmbeddedRedis
import com.google.inject.binder.ScopedBindingBuilder
import com.typesafe.config.{Config, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.filter.ConfPaths.{ConsumerConfPaths, FilterConfPaths, ProducerConfPaths}
import com.ubirch.filter.model.cache.{Cache, CacheStoreMock, CustomCache}
import com.ubirch.filter.model.eventlog.Finder
import com.ubirch.filter.model.{CassandraFinderAlwaysFound, Error, Values}
import com.ubirch.filter.services.config.ConfigProvider
import com.ubirch.filter.util.MessageEnvelopeGenerator.generateMsgEnvelope
import com.ubirch.filter.{Binder, EmbeddedCassandra, InjectorHelper, TestBase}
import com.ubirch.kafka.MessageEnvelope
import com.ubirch.kafka.util.PortGiver
import io.prometheus.client.CollectorRegistry
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.json4s.Formats
import org.scalatest._
import redis.embedded.RedisServer

import java.util.Base64
import java.util.concurrent.TimeoutException
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}
import scala.language.postfixOps

/**
  * This class provides all integration tests, except for those testing a missing redis connection on startup.
  * The Kafka config has to be inside each single test to enable parallel testing with different ports.
  */
class FilterServiceIntegrationTest extends TestBase with EmbeddedRedis with EmbeddedCassandra with LazyLogging with BeforeAndAfter {

  implicit val seMsgEnv: Serializer[MessageEnvelope] = com.ubirch.kafka.EnvelopeSerializer
  implicit val deMsgEnv: Deserializer[MessageEnvelope] = com.ubirch.kafka.EnvelopeDeserializer
  implicit val formats: Formats = FilterService.formats
  implicit val deRej: Deserializer[Error] = Error.ErrorDeserializer

  var redis: RedisServer = _

  /**
    * Overwrite default bootstrap server and topic values of the kafka consumer and producers
    */
  def customTestConfigProvider(bootstrapServers: String): ConfigProvider = new ConfigProvider {
    override def conf: Config = super.conf.withValue(
      ConsumerConfPaths.CONSUMER_BOOTSTRAP_SERVERS,
      ConfigValueFactory.fromAnyRef(bootstrapServers)
    ).withValue(
      ProducerConfPaths.PRODUCER_BOOTSTRAP_SERVERS,
      ConfigValueFactory.fromAnyRef(bootstrapServers)
    )
  }

  /**
    * Simple injector that replaces the kafka bootstrap server and topics to the given ones
    */
  def FakeSimpleInjector(bootstrapServers: String): InjectorHelper = new InjectorHelper(List(new Binder {
    override def Config: ScopedBindingBuilder = bind(classOf[Config]).toProvider(customTestConfigProvider(bootstrapServers))
  })) {}

  override protected def beforeAll(): Unit = {
    startCassandra()
    cassandra.executeScripts(eventLogCreationCassandraStatement)
  }

  override def afterAll(): Unit = {
    stopCassandra()
  }

  /**
    * Starting a new embedded redis server for testing purposes.
    */
  before {
    redis = new RedisServer(6379)
    Thread.sleep(1000)
    redis.start()
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
    Thread.sleep(1000)
  }

  override protected def beforeEach(): Unit = {
    CollectorRegistry.defaultRegistry.clear()
  }

  override protected def afterEach(): Unit = {
    while (EmbeddedKafka.isRunning) {
      logger.info("Kafka still running...")
      Thread.sleep(1000)
      EmbeddedKafka.stop()
    }
    logger.info(s"Embedded kafka status: isRunning = ${EmbeddedKafka.isRunning}")
  }

  "FilterService" must {

    "forward message first time and detect reply attack with help of redis cache when send a second time" in {
      implicit val kafkaConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)
      val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort

      withRunningKafka {
        Thread.sleep(1000)
        val msgEnvelope = generateMsgEnvelope()
        val Injector = FakeSimpleInjector(bootstrapServers)
        val conf = Injector.get[Config]

        //publish message first time
        publishToKafka(readConsumerTopicHead(conf), msgEnvelope)
        Thread.sleep(1000)
        val consumer = Injector.get[DefaultFilterService]
        consumer.consumption.startPolling()
        Thread.sleep(1000)
        consumeFirstMessageFrom[MessageEnvelope](readProducerForwardTopic(conf)).ubirchPacket.getUUID mustBe
          msgEnvelope.ubirchPacket.getUUID

        //publish message second time (replay attack)
        publishToKafka(readConsumerTopicHead(conf), msgEnvelope)

        val rejection = consumeFirstMessageFrom[Error](readProducerRejectionTopic(conf))
        rejection.causes mustBe List(Values.FOUND_IN_CACHE_MESSAGE)
        rejection.error mustBe Values.REPLAY_ATTACK_NAME
      }
    }

    "also detect replay attack when cache is down" in {
      implicit val kafkaConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)
      val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort

      def FakeInjector(bootstrapServers: String): InjectorHelper = new InjectorHelper(List(new Binder {
        override def Config: ScopedBindingBuilder = bind(classOf[Config]).toProvider(customTestConfigProvider(bootstrapServers))

        override def Finder: ScopedBindingBuilder = bind(classOf[Finder]).to(classOf[CassandraFinderAlwaysFound])
      })) {}

      withRunningKafka {
        val Injector = FakeInjector(bootstrapServers)
        val conf = Injector.get[Config]

        val fakeFilter = Injector.get[AbstractFilterService]
        fakeFilter.consumption.startPolling()
        Thread.sleep(1000)

        val msgEnvelope = generateMsgEnvelope()
        publishToKafka(readConsumerTopicHead(conf), msgEnvelope)

        val rejection = consumeFirstMessageFrom[Error](readProducerRejectionTopic(conf))
        rejection.causes mustBe List(Values.FOUND_IN_VERIFICATION_MESSAGE)
        rejection.error mustBe Values.REPLAY_ATTACK_NAME
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

        val Injector = FakeSimpleInjector(bootstrapServers)
        val conf = Injector.get[Config]

        publishToKafka[String](readConsumerTopicHead(conf), "unparseable example message")

        val consumer: DefaultFilterService = Injector.get[DefaultFilterService]
        consumer.consumption.startPolling()
        Thread.sleep(1000)

        val message: Error = consumeFirstMessageFrom[Error](readProducerErrorTopic(conf))
        message.microservice mustBe "filter-service"
        message.error mustBe "JsonParseException"
        message.causes mustBe List("unable to parse consumer record with key: null.")
        assertThrows[TimeoutException] {
          consumeFirstMessageFrom[MessageEnvelope](readProducerForwardTopic(conf))
        }
      }

    }

    "pause the consumption of new messages when there is an error sending messages" in {

      implicit val kafkaConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)
      val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort

      def FakeInjectorCustomCache(configProvider: ConfigProvider): InjectorHelper = new InjectorHelper(List(new Binder {
        override def Config: ScopedBindingBuilder = bind(classOf[Config]).toProvider(configProvider)

        override def FilterService: ScopedBindingBuilder = bind(classOf[AbstractFilterService]).to(classOf[ExceptionFilterServ])

        override def Cache: ScopedBindingBuilder = bind(classOf[Cache]).to(classOf[CustomCache])
      })) {}

      withRunningKafka {
        val Injector = FakeInjectorCustomCache(customTestConfigProvider(bootstrapServers))
        val conf = Injector.get[Config]

        val fakeFilterService = Injector.get[ExceptionFilterServ]

        val cache = Injector.get[Cache].asInstanceOf[CustomCache]
        fakeFilterService.consumption.startPolling()

        val msgEnvelope1 = generateMsgEnvelope(payload = "6767".getBytes)
        val msgEnvelope2 = generateMsgEnvelope(payload = "6868".getBytes)

        publishToKafka(readConsumerTopicHead(conf), msgEnvelope1)
        Thread.sleep(4000)
        publishToKafka(readConsumerTopicHead(conf), msgEnvelope2)
        Thread.sleep(4000)

        /**
          * the first two messages should be msgEnvelope1
          */
        assert(cache.list.head sameElements cache.list(1))

        /**
          * the last two messages should be msgEnvelope1 and msgEnvelope2 as the consumer repeats consuming the
          * not yet committed messages.
          */
        assert(!(cache.list.last sameElements cache.list(cache.list.size - 2)))
      }
    }

    "forward all messages when activeState equals false" in {
      implicit val kafkaConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)
      val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort

      def FakeInjector(bootstrapServers: String): InjectorHelper = new InjectorHelper(List(new Binder {
        override def Config: ScopedBindingBuilder = bind(classOf[Config]).toProvider(new ConfigProvider {
          override def conf: Config = super.conf.withValue(
            ConsumerConfPaths.CONSUMER_BOOTSTRAP_SERVERS,
            ConfigValueFactory.fromAnyRef(bootstrapServers)
          ).withValue(
            ProducerConfPaths.PRODUCER_BOOTSTRAP_SERVERS,
            ConfigValueFactory.fromAnyRef(bootstrapServers)
          ).withValue(
            FilterConfPaths.FILTER_STATE,
            ConfigValueFactory.fromAnyRef(false)
          )
        })
      })) {}

      withRunningKafka {

        val Injector = FakeInjector(bootstrapServers)
        val conf = Injector.get[Config]

        val filter = Injector.get[AbstractFilterService]
        filter.consumption.startPolling()

        val msgEnvelope = generateMsgEnvelope()
        publishToKafka(readConsumerTopicHead(conf), msgEnvelope)

        val forwardedMessage = consumeFirstMessageFrom[MessageEnvelope](readProducerForwardTopic(conf))
        forwardedMessage.ubirchPacket.getUUID mustEqual msgEnvelope.ubirchPacket.getUUID
      }
    }

    /**
      * Here we set the filter state to non active (should always forward the message) BUT the env = prod, so it should
      * still check if redis / cassi is up or not and forward accordingly
      */
    "not forward all messages when activeState equals false and env = prod" in {
      implicit val kafkaConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)
      val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort

      def FakeInjector(bootstrapServers: String): InjectorHelper = new InjectorHelper(List(new Binder {
        override def Config: ScopedBindingBuilder = bind(classOf[Config]).toProvider(new ConfigProvider {
          override def conf: Config = super.conf.withValue(
            ConsumerConfPaths.CONSUMER_BOOTSTRAP_SERVERS,
            ConfigValueFactory.fromAnyRef(bootstrapServers)
          ).withValue(
            ProducerConfPaths.PRODUCER_BOOTSTRAP_SERVERS,
            ConfigValueFactory.fromAnyRef(bootstrapServers)
          ).withValue(
            FilterConfPaths.ENVIRONMENT,
            ConfigValueFactory.fromAnyRef(Values.PRODUCTION_NAME)
          ).withValue(
            FilterConfPaths.FILTER_STATE,
            ConfigValueFactory.fromAnyRef(false)
          )
        })

        override def Cache: ScopedBindingBuilder = bind(classOf[Cache]).to(classOf[CacheStoreMock])
      })) {}

      withRunningKafka {

        val Injector = FakeInjector(bootstrapServers)
        val conf = Injector.get[Config]
        val filter = Injector.get[AbstractFilterService]
        filter.consumption.startPolling()
        val msgEnvelope = generateMsgEnvelope()
        publishToKafka(readConsumerTopicHead(conf), msgEnvelope)
        Thread.sleep(1000)
        val rejection = consumeFirstMessageFrom[Error](readProducerRejectionTopic(conf))
        rejection.causes mustBe List(Values.FOUND_IN_CACHE_MESSAGE)
        rejection.error mustBe Values.REPLAY_ATTACK_NAME
      }
    }
  }

  "Verification Lookup" must {

    "return None if hash hasn't been found in Cassandra/ processed yet." in {
      val cr = new ConsumerRecord[String, String]("topic", 1, 1, "key", "Teststring")
      val msgEnv = generateMsgEnvelope()
      val data = ProcessingData(cr, msgEnv.ubirchPacket)
      val Injector = FakeSimpleInjector(ConsumerConfPaths.CONSUMER_BOOTSTRAP_SERVERS)
      val filterConsumer = Injector.get[DefaultFilterService]
      val result = Await.result(filterConsumer.makeVerificationLookup(data), 5.seconds)
      implicit val ec: ExecutionContext = Injector.get[ExecutionContext]
      result match {
        case Some(_) => fail()
        case None =>
      }
    }
  }

  "Payload encoding" must {

    "be encoded when retrieved with asText() and decoded when retrieved with binaryValue()" in {
      val payload =
        "v1jjADSpJFPl7vTg8gsiui2aTOCL1v4nYrmiTePvcxNBt1x/+JoApHOLa4rjGGEz72PusvGXbF9t6qe8Kbck/w=="
      val decodedPayload = Base64.getDecoder.decode(payload)
      val envelope = generateMsgEnvelope(payload = payload)
      assert(envelope.ubirchPacket.getPayload.asText() == payload)
      assert(envelope.ubirchPacket.getPayload.binaryValue() sameElements decodedPayload)
    }
  }

}

