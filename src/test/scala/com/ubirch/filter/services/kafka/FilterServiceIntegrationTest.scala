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

import java.util.{Base64, UUID}
import java.util.concurrent.TimeoutException

import com.github.nosan.embedded.cassandra.cql.CqlScript
import com.github.sebruck.EmbeddedRedis
import com.google.inject.binder.ScopedBindingBuilder
import com.typesafe.config.{Config, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.filter.{Binder, EmbeddedCassandra, InjectorHelper, TestBase}
import com.ubirch.filter.cache.{Cache, CacheMockAlwaysFalse, CacheMockAlwaysTrue}
import com.ubirch.filter.model._
import com.ubirch.filter.services.config.ConfigProvider
import com.ubirch.filter.util.Messages
import com.ubirch.filter.ConfPaths.{ConsumerConfPaths, FilterConfPaths, ProducerConfPaths}
import com.ubirch.filter.model.eventlog.{CassandraFinder, Finder}
import com.ubirch.filter.models.CassandraFinderAlwaysFound
import com.ubirch.filter.services.Lifecycle
import com.ubirch.kafka.MessageEnvelope
import com.ubirch.kafka.util.PortGiver
import com.ubirch.protocol.ProtocolMessage
import io.prometheus.client.CollectorRegistry
import javax.inject.{Inject, Singleton}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.json4s.JsonAST._
import org.scalatest._
import os.proc
import redis.embedded.RedisServer

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.postfixOps


/**
 * This class provides all integration tests, except for those testing a missing redis connection on startup.
 * The Kafka config has to be inside each single test to enable parallel testing with different ports.
 */
class FilterServiceIntegrationTest extends WordSpec with TestBase with EmbeddedRedis with EmbeddedCassandra with LazyLogging with BeforeAndAfter {

  implicit val seMsgEnv: Serializer[MessageEnvelope] = com.ubirch.kafka.EnvelopeSerializer
  implicit val deMsgEnv: Deserializer[MessageEnvelope] = com.ubirch.kafka.EnvelopeDeserializer
  implicit val deRej: Deserializer[Rejection] = RejectionDeserializer
  implicit val deError: Deserializer[FilterError] = FilterErrorDeserializer

  var redis: RedisServer = _

  /**
  * Overwrite default bootstrap server and topic values of the kafka consumer and producers
    */
  def customTestConfigProvider(bootstrapServers: String): ConfigProvider = new ConfigProvider {
    override def conf: Config = super.conf.withValue(
      ConsumerConfPaths.BOOTSTRAP_SERVERS,
      ConfigValueFactory.fromAnyRef(bootstrapServers)
    ).withValue(
      ProducerConfPaths.BOOTSTRAP_SERVERS,
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
   * Method killing any embedded redis application, in case an earlier test was aborted without executing after.
   * Starting a new embedded redis server for testing purposes.
   */
  before {
    try {
      val embeddedRedisPid = getPidOfServiceUsingGivenPort(6379)
      proc("kill", "-9", embeddedRedisPid).call()
    } catch {
      case _: Throwable =>
    }
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
    Thread.sleep(10000)
  }

  override protected def beforeEach(): Unit = {
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
        val Injector = FakeSimpleInjector(bootstrapServers)
        val conf = Injector.get[Config]
        publishToKafka(readConsumerTopicHead(conf), msgEnvelope)
        Thread.sleep(1000)
        val consumer = Injector.get[DefaultFilterService]
        consumer.consumption.startPolling()
        Thread.sleep(1000)
        consumeFirstMessageFrom[MessageEnvelope](readProducerForwardTopic(conf)).ubirchPacket.getUUID mustBe
          msgEnvelope.ubirchPacket.getUUID

        //publish message second time (replay attack)
        publishToKafka(readConsumerTopicHead(conf), msgEnvelope)

        val rejection = consumeFirstMessageFrom[Rejection](readProducerRejectionTopic(conf))
        rejection.message mustBe Messages.foundInCacheMsg
        rejection.rejectionName mustBe Messages.replayAttackName
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

        val msgEnvelope = generateMessageEnvelope()
        publishToKafka(readConsumerTopicHead(conf), msgEnvelope)

        val rejection = consumeFirstMessageFrom[Rejection](readProducerRejectionTopic(conf))
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

        val Injector = FakeSimpleInjector(bootstrapServers)
        val conf = Injector.get[Config]

        publishToKafka[String](readConsumerTopicHead(conf), "unparseable example message")


        val consumer: DefaultFilterService = Injector.get[DefaultFilterService]
        consumer.consumption.startPolling()
        Thread.sleep(1000)

        val message: FilterError = consumeFirstMessageFrom[FilterError](readProducerErrorTopic(conf))
        message.serviceName mustBe "filter-service"
        message.exceptionName mustBe "JsonParseException"
        message.message mustBe "unable to parse consumer record with key: null."
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

        val fakeFilterService = Injector.get[AbstractFilterService]

        val cache = Injector.get[Cache].asInstanceOf[CustomCache]
        fakeFilterService.consumption.startPolling()

        val msgEnvelope1 = generateMessageEnvelope()
        val msgEnvelope2 = generateMessageEnvelope()

        publishToKafka(readConsumerTopicHead(conf), msgEnvelope1)
        Thread.sleep(5000)
        publishToKafka(readConsumerTopicHead(conf), msgEnvelope2)

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

      def FakeInjector(bootstrapServers: String): InjectorHelper = new InjectorHelper(List(new Binder {
        override def Config: ScopedBindingBuilder = bind(classOf[Config]).toProvider(new ConfigProvider {
          override def conf: Config = super.conf.withValue(
            ConsumerConfPaths.BOOTSTRAP_SERVERS,
            ConfigValueFactory.fromAnyRef(bootstrapServers)
          ).withValue(
            ProducerConfPaths.BOOTSTRAP_SERVERS,
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

        val msgEnvelope = generateMessageEnvelope()
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
            ConsumerConfPaths.BOOTSTRAP_SERVERS,
            ConfigValueFactory.fromAnyRef(bootstrapServers)
          ).withValue(
            ProducerConfPaths.BOOTSTRAP_SERVERS,
            ConfigValueFactory.fromAnyRef(bootstrapServers)
          ).withValue(
            FilterConfPaths.ENVIRONMENT,
            ConfigValueFactory.fromAnyRef(Values.PRODUCTION_NAME)
          ).withValue(
            FilterConfPaths.FILTER_STATE,
            ConfigValueFactory.fromAnyRef(false)
          )
        })
        override def Cache: ScopedBindingBuilder = bind(classOf[Cache]).to(classOf[CacheMockAlwaysTrue])
      })) {}


      withRunningKafka {

        val Injector = FakeInjector(bootstrapServers)
        val conf = Injector.get[Config]
        val filter = Injector.get[AbstractFilterService]
        filter.consumption.startPolling()

        val msgEnvelope = generateMessageEnvelope()
        publishToKafka(readConsumerTopicHead(conf), msgEnvelope)

        Thread.sleep(1000)

        val rejection = consumeFirstMessageFrom[Rejection](readProducerRejectionTopic(conf))
        rejection.message mustBe Messages.foundInCacheMsg
        rejection.rejectionName mustBe Messages.replayAttackName
      }
    }
  }

  "Verification Lookup" must {

    "return NotFound if hash hasn't been processed yet." in {
      val cr = new ConsumerRecord[String, String]("topic", 1, 1, "key", "Teststring")
      val payload = UUID.randomUUID().toString
      val data = ProcessingData(cr, payload)
      val Injector = FakeSimpleInjector(ConsumerConfPaths.BOOTSTRAP_SERVERS)
      val filterConsumer = Injector.get[DefaultFilterService]
      val result = Await.result(filterConsumer.makeVerificationLookup(data), 5.second)
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
      val envelope = generateMessageEnvelope(payload)
      assert(envelope.ubirchPacket.getPayload.asText() == payload)
      assert(envelope.ubirchPacket.getPayload.binaryValue() sameElements decodedPayload)
    }
  }

  "Cassandra lookup" must {

    /**
      * This test insert a certain value in cassandra (the payload) and verify that the filter finds it
      */
    "consume and process successfully when Found" in {

      cassandra.executeScripts(
        CqlScript.statements(
          insertEventSql
        )
      )

      implicit val kafkaConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)

      val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort

      def testInjector(bootstrapServers: String): InjectorHelper = new InjectorHelper(List(new Binder {
        override def Config: ScopedBindingBuilder = bind(classOf[Config]).toProvider(customTestConfigProvider(bootstrapServers))
        override def Cache: ScopedBindingBuilder = bind(classOf[Cache]).to(classOf[CacheMockAlwaysFalse])
      })) {}

      withRunningKafka {
        val msgEnvelope = generateMessageEnvelope(payload)
        val Injector = testInjector(bootstrapServers)
        val conf = Injector.get[Config]
        publishToKafka(readConsumerTopicHead(conf), msgEnvelope)
        Thread.sleep(1000)
        val consumer = Injector.get[DefaultFilterService]
        consumer.consumption.startPolling()
        Thread.sleep(1000)

        val rejection = consumeFirstMessageFrom[Rejection](readProducerRejectionTopic(conf))
        rejection.message mustBe Messages.foundInVerificationMsg
        rejection.rejectionName mustBe Messages.replayAttackName
      }


    }

    "consume and process successfully when not found" in {
      cassandra.executeScripts(
        CqlScript.statements(
          insertEventSql
        )
      )

      implicit val kafkaConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)

      val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort

      def testInjector(bootstrapServers: String): InjectorHelper = new InjectorHelper(List(new Binder {
        override def Config: ScopedBindingBuilder = bind(classOf[Config]).toProvider(customTestConfigProvider(bootstrapServers))
        override def Cache: ScopedBindingBuilder = bind(classOf[Cache]).to(classOf[CacheMockAlwaysFalse])
      })) {}

      withRunningKafka {
        val msgEnvelope = generateMessageEnvelope(payload + ",")
        val Injector = testInjector(bootstrapServers)
        val conf = Injector.get[Config]
        publishToKafka(readConsumerTopicHead(conf), msgEnvelope)
        Thread.sleep(1000)
        val consumer = Injector.get[DefaultFilterService]
        consumer.consumption.startPolling()
        Thread.sleep(1000)

        try {
          consumeFirstMessageFrom[Rejection](readProducerRejectionTopic(conf))
          fail()
        } catch {
          case _: java.util.concurrent.TimeoutException =>
          case _: Throwable => fail()
        }
      }

    }
  }

  val payload = "c29tZSBieXRlcyEAAQIDnw=="

  val insertEventSql: String =
    s"""
       |INSERT INTO events (id, customer_id, service_class, category, event, event_time, year, month, day, hour, minute, second, milli, signature, nonce)
       | VALUES ('$payload', 'customer_id', 'service_class', '${Values.UPP_CATEGORY}', '{
       |   "hint":0,
       |   "payload":"$payload",
       |   "signature":"5aTelLQBerVT/vJiL2qjZCxWxqlfwT/BaID0zUVy7LyUC9nUdb02//aCiZ7xH1HglDqZ0Qqb7GyzF4jtBxfSBg==",
       |   "signed":"lRKwjni1ymWXEeiBhcg+pwAOTQCwc29tZSBieXRlcyEAAQIDnw==",
       |   "uuid":"8e78b5ca-6597-11e8-8185-c83ea7000e4d",
       |   "version":34
       |}', '2019-01-29T17:00:28.333Z', 2019, 5, 2, 19, 439, 16, 0, '0681D35827B17104A2AACCE5A08C4CD1BC8A5EF5EFF4A471D15976693CC0D6D67392F1CACAE63565D6E521D2325A998CDE00A2FEF5B65D0707F4158000EB6D05',
       |'34376336396166392D336533382D343665652D393063332D616265313364383335353266');
    """.stripMargin

}

// Some classes have to be defined outside of the functions that uses them, as Guice does not support injecting into inner classes

/**
  * A fake filter service that always throws an exception, when the send() method is called.
  *
  * @param cache The cache is only used to record the messages being processed in this test.
  */
@Singleton
class ExceptionFilterServ @Inject()(cache: Cache, cassandraFinder: CassandraFinder, config: Config, lifecycle: Lifecycle)(override implicit val ec: ExecutionContext) extends AbstractFilterService(cache, cassandraFinder, config, lifecycle) {
  override def send(topic: String, value: String): Future[RecordMetadata] = {
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


