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

import java.util.{ Base64, UUID }
import java.util.concurrent.TimeoutException

import com.github.sebruck.EmbeddedRedis
import com.google.inject.binder.ScopedBindingBuilder
import com.typesafe.config.{ Config, ConfigValueFactory }
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.filter.{ Binder, EmbeddedCassandra, InjectorHelper, TestBase }
import com.ubirch.filter.model.{ FilterError, FilterErrorDeserializer, Rejection, RejectionDeserializer }
import com.ubirch.filter.services.config.ConfigProvider
import com.ubirch.filter.ConfPaths.{ ConsumerConfPaths, ProducerConfPaths }
import com.ubirch.kafka.MessageEnvelope
import com.ubirch.kafka.util.PortGiver
import com.ubirch.protocol.ProtocolMessage
import io.prometheus.client.CollectorRegistry
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.kafka.common.serialization.{ Deserializer, Serializer }
import org.json4s.JsonAST.{ JObject, JString }
import org.scalatest.{ BeforeAndAfter, Ignore }
import redis.embedded.RedisServer

import scala.language.postfixOps
import scala.sys.process._
import scala.util.Try

// TODO: RESTORE: remove this tag
@Ignore
class FilterServiceIntegrationTestWithoutCache extends TestBase with EmbeddedRedis with EmbeddedCassandra with LazyLogging with BeforeAndAfter {

  implicit val seMsgEnv: Serializer[MessageEnvelope] = com.ubirch.kafka.EnvelopeSerializer
  implicit val deMsgEnv: Deserializer[MessageEnvelope] = com.ubirch.kafka.EnvelopeDeserializer
  implicit val deRej: Deserializer[Rejection] = RejectionDeserializer
  implicit val deError: Deserializer[FilterError] = FilterErrorDeserializer

  override protected def beforeAll(): Unit = {
    startCassandra()
    cassandra.executeScripts(eventLogCreationCassandraStatement)
  }

  override def afterAll(): Unit = {
    stopCassandra()
    Thread.sleep(5000)
  }

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
    * Simple Guice injector that mimicate the default one but replace the bootstrap and consumer topics by the provided one
    */
  def FakeSimpleInjector(bootstrapServers: String): InjectorHelper = new InjectorHelper(List(new Binder {
    override def Config: ScopedBindingBuilder = bind(classOf[Config]).toProvider(customTestConfigProvider(bootstrapServers))
  })) {}

  override protected def beforeEach(): Unit = {
    CollectorRegistry.defaultRegistry.clear()
    EmbeddedKafka.stop()
    EmbeddedKafka.stopZooKeeper()
    logger.info(s"Embedded kafka status: isRunning = ${EmbeddedKafka.isRunning}")
    try {
      redis.stop()
    } catch {
      case _: Throwable =>
    }
  }

  override protected def afterEach(): Unit = {
    EmbeddedKafka.stop()
    logger.info(s"Embedded kafka status: isRunning = ${EmbeddedKafka.isRunning}")
  }

  var redis: RedisServer = _

  /**
    * Start and return a DefaultFilterService as well as its config
    */
  def startKafka(bootstrapServers: String): (FilterService, Config) = {

    val Injector = FakeSimpleInjector(bootstrapServers)
    val consumer = Injector.get[DefaultFilterService]
    val conf = Injector.get[Config]
    consumer.consumption.startPolling()
    (consumer, conf)
  }

  "Missing redis connection" must {

    "should cause error messages" in {
      implicit val kafkaConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)
      val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort

      withRunningKafka {

        logger.info("trying stop redis")
        try {
          redis.stop()
        } catch {
          case _: Throwable =>
        }

        val msgEnvelope = generateMessageEnvelope()

        val (_, conf) = startKafka(bootstrapServers)

        //publish message first time
        publishToKafka(readConsumerTopicHead(conf), msgEnvelope)

        Thread.sleep(3000)

        consumeFirstMessageFrom[MessageEnvelope](readProducerForwardTopic(conf)).ubirchPacket.getUUID mustBe
          msgEnvelope.ubirchPacket.getUUID

        val cacheError1 = consumeFirstMessageFrom[FilterError](readProducerErrorTopic(conf))
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
        val (_, conf) = startKafka(bootstrapServers)
        publishToKafka(readConsumerTopicHead(conf), msgEnvelope)
        val cacheError1 = consumeFirstMessageFrom[FilterError](readProducerErrorTopic(conf))
        cacheError1.exceptionName mustBe "NoCacheConnectionException"
        consumeFirstMessageFrom[MessageEnvelope](readProducerForwardTopic(conf)).ubirchPacket.getUUID mustBe
          msgEnvelope.ubirchPacket.getUUID

        //publish message second time (replay attack)
        publishToKafka(readConsumerTopicHead(conf), msgEnvelope)

        val cacheError2 = consumeFirstMessageFrom[FilterError](readProducerErrorTopic(conf))
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
        val (_, conf) = startKafka(bootstrapServers)
        publishToKafka(readConsumerTopicHead(conf), msgEnvelope1)
        consumeFirstMessageFrom[MessageEnvelope](readProducerForwardTopic(conf)).ubirchPacket.getUUID mustBe
          msgEnvelope1.ubirchPacket.getUUID
        val cacheError1 = consumeFirstMessageFrom[FilterError](readProducerErrorTopic(conf))
        cacheError1.exceptionName mustBe "NoCacheConnectionException"
        redis = new RedisServer(6379)
        redis.start()
        Thread.sleep(8000)

        //publish second message time
        val msgEnvelope2 = generateMessageEnvelope()
        publishToKafka(readConsumerTopicHead(conf), msgEnvelope2)

        assertThrows[TimeoutException] {
          consumeNumberMessagesFrom[FilterError](readProducerErrorTopic(conf), 2)
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
