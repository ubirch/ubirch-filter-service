package com.ubirch.filter.services.kafka

import com.github.nosan.embedded.cassandra.cql.CqlScript
import com.google.inject.binder.ScopedBindingBuilder
import com.typesafe.config.{Config, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.filter.ConfPaths.{ConsumerConfPaths, ProducerConfPaths}
import com.ubirch.filter.model.cache.{Cache, CacheMockAlwaysFalse}
import com.ubirch.filter.model.{Error, Values}
import com.ubirch.filter.services.config.ConfigProvider
import com.ubirch.filter.testUtils.MessageEnvelopeGenerator.generateMsgEnvelope
import com.ubirch.filter.{Binder, EmbeddedCassandra, InjectorHelper, TestBase}
import com.ubirch.kafka.MessageEnvelope
import com.ubirch.kafka.util.PortGiver
import io.prometheus.client.CollectorRegistry
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.json4s.Formats
import org.scalatest.BeforeAndAfter
import redis.embedded.RedisServer

import java.util.UUID
import java.util.concurrent.TimeoutException

class CassandraFallbackIntegrationTest extends TestBase with EmbeddedCassandra with LazyLogging with BeforeAndAfter {

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
    * Called after all tests. Not working always unfortunately.
    */
  after {
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

  "Cassandra lookup" must {

    /**
      * This test insert a certain value in cassandra (the payload) and verify that the filter finds it
      */
    "consume and processed as replayAttack when Found if not of type UPP update" in {
      val payload = "c29tZSBieXRlcyEAAQIDnw=="

      cassandra.executeScripts(
        CqlScript.statements(
          insertEventSql(payload = payload)
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
        val msgEnvelope = generateMsgEnvelope(payload = payload)
        val Injector = testInjector(bootstrapServers)
        val conf = Injector.get[Config]
        publishToKafka(readConsumerTopicHead(conf), msgEnvelope)
        Thread.sleep(1000)
        val consumer = Injector.get[DefaultFilterService]
        consumer.consumption.startPolling()
        Thread.sleep(1000)

        val rejection = consumeFirstMessageFrom[Error](readProducerRejectionTopic(conf))
        rejection.causes mustBe List(Values.FOUND_IN_VERIFICATION_MESSAGE)
        rejection.error mustBe Values.REPLAY_ATTACK_NAME

        assertThrows[TimeoutException] {
          consumeFirstMessageFrom[MessageEnvelope](readProducerForwardTopic(conf))
        }
      }
    }

    "consume and process successfully when not found if not of type UPP update" in {
      val payload = "c29tZSBieXRlcyEAAQIDnw=="

      cassandra.executeScripts(
        CqlScript.statements(
          insertEventSql(payload, "0")
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
        val msgEnvelope = generateMsgEnvelope(payload = payload + ",")
        val Injector = testInjector(bootstrapServers)
        val conf = Injector.get[Config]
        publishToKafka(readConsumerTopicHead(conf), msgEnvelope)
        Thread.sleep(1000)
        val consumer = Injector.get[DefaultFilterService]
        consumer.consumption.startPolling()
        Thread.sleep(1000)

        try {
          consumeFirstMessageFrom[Error](readProducerRejectionTopic(conf))
          fail()
        } catch {
          case _: java.util.concurrent.TimeoutException =>
          case _: Throwable => fail()
        }
      }
    }

    "consume and forward successfully when Found if of type UPP update (hint = 250, 251, 252)" in {

      val uuid1 = UUID.randomUUID()
      val uuid2 = UUID.randomUUID()
      val uuid3 = UUID.randomUUID()
      val list = List((uuid1, "c29tZSBieXRlcyEAAQIDnw==", 250), (uuid2, "hellohello", 251), (uuid3, "byebye", 252))

      def addToCassandra(uuid: UUID, payload: String, hint: Int): Unit = {
        cassandra.executeScripts(
          CqlScript.statements(
            insertEventSql(uuid.toString, payload, hint)
          )
        )
      }

      list.foreach(p => addToCassandra(p._1, p._2, p._3))

      implicit val kafkaConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)

      val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort

      def testInjector(bootstrapServers: String): InjectorHelper = new InjectorHelper(List(new Binder {
        override def Config: ScopedBindingBuilder = bind(classOf[Config]).toProvider(customTestConfigProvider(bootstrapServers))

        override def Cache: ScopedBindingBuilder = bind(classOf[Cache]).to(classOf[CacheMockAlwaysFalse])
      })) {}

      withRunningKafka {
        val Injector = testInjector(bootstrapServers)
        val conf = Injector.get[Config]

        val msgEnvelopes = list.map(p => generateMsgEnvelope(uuid = p._1, payload = p._2, hint = p._3))

        msgEnvelopes.foreach(e => publishToKafka(readConsumerTopicHead(conf), e))
        val consumer = Injector.get[DefaultFilterService]
        consumer.consumption.startPolling()
        Thread.sleep(8000)

        val allUUIDS = list.map(_._1).toSet
        val forwardedMsgs = consumeNumberMessagesFrom[MessageEnvelope](readProducerForwardTopic(conf), 3)
        forwardedMsgs.map(msgEnv => assert(allUUIDS.contains(msgEnv.ubirchPacket.getUUID)))
        //Attention: More comparison than same hash is never done, when making verification/ cassandra lookup

        assertThrows[TimeoutException] {
          consumeFirstMessageFrom[MessageEnvelope](readProducerRejectionTopic(conf))
        }
      }
    }

    "consume and trigger replayAttack warning when NOT Found if of type UPP update (hint = 250, 251, 252)" in {

      val uuid1 = UUID.randomUUID()
      val uuid2 = UUID.randomUUID()
      val uuid3 = UUID.randomUUID()
      val list = List((uuid1, "c29tZSBieXRlcyEAAQIDnw==", 250), (uuid2, "hellohello", 251), (uuid3, "byebye", 252))

      implicit val kafkaConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)

      val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort

      def testInjector(bootstrapServers: String): InjectorHelper = new InjectorHelper(List(new Binder {
        override def Config: ScopedBindingBuilder = bind(classOf[Config]).toProvider(customTestConfigProvider(bootstrapServers))

        override def Cache: ScopedBindingBuilder = bind(classOf[Cache]).to(classOf[CacheMockAlwaysFalse])
      })) {}

      withRunningKafka {
        val Injector = testInjector(bootstrapServers)
        val conf = Injector.get[Config]

        val msgEnvelopes = list.map(p => generateMsgEnvelope(uuid = p._1, payload = p._2, hint = p._3))

        msgEnvelopes.foreach(e => publishToKafka(readConsumerTopicHead(conf), e))
        val consumer = Injector.get[DefaultFilterService]
        consumer.consumption.startPolling()
        Thread.sleep(8000)


        val rejectionMsgs = consumeNumberMessagesFrom[Error](readProducerRejectionTopic(conf), 3)
        rejectionMsgs.map { rejection =>
          rejection.error mustBe Values.REPLAY_ATTACK_NAME
          rejection.causes mustBe List(Values.NOT_FOUND_IN_VERIFICATION_MESSAGE)
        }

        assertThrows[TimeoutException] {
          consumeFirstMessageFrom[MessageEnvelope](readProducerForwardTopic(conf))
        }
      }
    }
  }

  def insertEventSql(uuid: String = UUID.randomUUID().toString, payload: String, hint: Int = 0): String =
    s"""
       |INSERT INTO events (id, customer_id, service_class, category, event, event_time, year, month, day, hour, minute, second, milli, signature, nonce)
       | VALUES ('$payload', 'customer_id', 'service_class', '${Values.UPP_CATEGORY}', '{
       |   "hint":$hint,
       |   "payload":"$payload",
       |   "signature":"5aTelLQBerVT/vJiL2qjZCxWxqlfwT/BaID0zUVy7LyUC9nUdb02//aCiZ7xH1HglDqZ0Qqb7GyzF4jtBxfSBg==",
       |   "signed":"lRKwjni1ymWXEeiBhcg+pwAOTQCwc29tZSBieXRlcyEAAQIDnw==",
       |   "uuid":$uuid,
       |   "version":34
       |}', '2019-01-29T17:00:28.333Z', 2019, 5, 2, 19, 439, 16, 0, '0681D35827B17104A2AACCE5A08C4CD1BC8A5EF5EFF4A471D15976693CC0D6D67392F1CACAE63565D6E521D2325A998CDE00A2FEF5B65D0707F4158000EB6D05',
       |'34376336396166392D336533382D343665652D393063332D616265313364383335353266');
    """.stripMargin

}
