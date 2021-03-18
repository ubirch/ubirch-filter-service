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
import com.typesafe.config.{ Config, ConfigValueFactory }
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.filter.ConfPaths.{ ConsumerConfPaths, ProducerConfPaths }
import com.ubirch.filter.model.Error
import com.ubirch.filter.services.config.ConfigProvider
import com.ubirch.filter.testUtils.MessageEnvelopeGenerator.generateMsgEnvelope
import com.ubirch.filter.{ Binder, EmbeddedCassandra, InjectorHelper, TestBase }
import com.ubirch.kafka.MessageEnvelope
import com.ubirch.kafka.util.PortGiver
import io.prometheus.client.CollectorRegistry
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.kafka.common.serialization.{ Deserializer, Serializer }
import org.json4s.Formats
import org.scalatest.BeforeAndAfter
import redis.embedded.RedisServer

import java.util.concurrent.TimeoutException
import scala.language.postfixOps
import scala.sys.process._

class FilterServiceIntegrationTestWithoutCache extends TestBase with EmbeddedRedis with EmbeddedCassandra with LazyLogging with BeforeAndAfter {

  implicit val seMsgEnv: Serializer[MessageEnvelope] = com.ubirch.kafka.EnvelopeSerializer
  implicit val deMsgEnv: Deserializer[MessageEnvelope] = com.ubirch.kafka.EnvelopeDeserializer
  implicit val formats: Formats = FilterService.formats
  implicit val deRej: Deserializer[Error] = Error.ErrorDeserializer

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
      ConsumerConfPaths.CONSUMER_BOOTSTRAP_SERVERS,
      ConfigValueFactory.fromAnyRef(bootstrapServers)
    ).withValue(
        ProducerConfPaths.PRODUCER_BOOTSTRAP_SERVERS,
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

    "cause error messages" in {
      implicit val kafkaConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)
      val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort

      withRunningKafka {

        try {
          redis.stop()
        } catch {
          case _: Throwable =>
        }

        val msgEnvelope = generateMsgEnvelope()

        val (_, conf) = startKafka(bootstrapServers)

        //publish message first time
        publishToKafka(readConsumerTopicHead(conf), msgEnvelope)
        Thread.sleep(3000)

        consumeFirstMessageFrom[MessageEnvelope](readProducerForwardTopic(conf)).ubirchPacket.getUUID mustBe
          msgEnvelope.ubirchPacket.getUUID
        val cacheError1 = consumeFirstMessageFrom[Error](readProducerErrorTopic(conf))
        cacheError1.error mustBe "NoCacheConnectionException"
        cacheError1.causes mustBe List("unable to make cache lookup 'null'.")
        val cacheError2 = consumeFirstMessageFrom[Error](readProducerErrorTopic(conf))
        cacheError2.error mustBe "NoCacheConnectionException"
        cacheError2.causes mustBe List("unable to add value for hash ODkzMTk= to cache.")

      }
    }

    "not change default behaviour when nothing is found in cassandra" in {
      implicit val kafkaConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)
      val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort

      withRunningKafka {

        try {
          redis.stop()
        } catch {
          case _: Throwable =>
        }

        val msgEnvelope = generateMsgEnvelope()
        //publish message first time
        val (_, conf) = startKafka(bootstrapServers)
        publishToKafka(readConsumerTopicHead(conf), msgEnvelope)
        val cacheError1 = consumeFirstMessageFrom[Error](readProducerErrorTopic(conf))
        cacheError1.error mustBe "NoCacheConnectionException"
        cacheError1.causes mustBe List("unable to make cache lookup 'null'.")
        val cacheError2 = consumeFirstMessageFrom[Error](readProducerErrorTopic(conf))
        cacheError2.error mustBe "NoCacheConnectionException"
        cacheError2.causes mustBe List("unable to add value for hash ODkzMTk= to cache.")

        val forwardedMsg1 = consumeFirstMessageFrom[MessageEnvelope](readProducerForwardTopic(conf))
        forwardedMsg1.ubirchPacket.getUUID mustBe msgEnvelope.ubirchPacket.getUUID

        //publish message second time (replay attack)
        publishToKafka(readConsumerTopicHead(conf), msgEnvelope)

        val forwardedMsg2 = consumeFirstMessageFrom[MessageEnvelope](readProducerForwardTopic(conf))
        forwardedMsg2.ubirchPacket.getUUID mustBe msgEnvelope.ubirchPacket.getUUID
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

        val msgEnvelope1 = generateMsgEnvelope()

        //publish first message
        val (_, conf) = startKafka(bootstrapServers)
        publishToKafka(readConsumerTopicHead(conf), msgEnvelope1)
        consumeFirstMessageFrom[MessageEnvelope](readProducerForwardTopic(conf)).ubirchPacket.getUUID mustBe
          msgEnvelope1.ubirchPacket.getUUID
        val cacheError = consumeNumberMessagesFrom[Error](readProducerErrorTopic(conf), 2)
        cacheError.head.error mustBe "NoCacheConnectionException"
        cacheError.head.causes mustBe List("unable to make cache lookup 'null'.")

        redis = new RedisServer(6379)
        redis.start()
        Thread.sleep(8000)

        //publish second message time
        val msgEnvelope2 = generateMsgEnvelope()
        publishToKafka(readConsumerTopicHead(conf), msgEnvelope2)

        assertThrows[TimeoutException] {
          consumeNumberMessagesFrom[Error](readProducerErrorTopic(conf), 2)
        }
        redis.stop()
      }
    }
  }

}
