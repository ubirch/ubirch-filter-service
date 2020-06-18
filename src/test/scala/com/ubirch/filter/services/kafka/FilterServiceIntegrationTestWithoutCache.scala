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

import com.github.sebruck.EmbeddedRedis
import com.google.inject.binder.ScopedBindingBuilder
import com.typesafe.config.{Config, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.filter.{Binder, EmbeddedCassandra, InjectorHelper, TestBase}
import com.ubirch.filter.model.{FilterError, FilterErrorDeserializer, Rejection, RejectionDeserializer}
import com.ubirch.filter.services.config.ConfigProvider
import com.ubirch.filter.util.Messages
import com.ubirch.filter.ConfPaths.{ConsumerConfPaths, ProducerConfPaths}
import com.ubirch.kafka.MessageEnvelope
import com.ubirch.kafka.util.PortGiver
import com.ubirch.protocol.ProtocolMessage
import io.prometheus.client.CollectorRegistry
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.json4s.JsonAST.{JObject, JString}
import org.scalatest.BeforeAndAfter
import os.proc
import redis.embedded.RedisServer

import scala.language.postfixOps
import scala.sys.process._

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

  /**
  * Simple Guice injector that mimicate the default one but replace the bootstrap and consumer topics by the provided one
    */
  def FakeSimpleInjector(bootstrapServers: String, consumerTopic: String): InjectorHelper = new InjectorHelper(List(new Binder {
    override def Config: ScopedBindingBuilder = bind(classOf[Config]).toProvider(customTestConfigProvider(bootstrapServers, consumerTopic))
  })) {}

  override protected def beforeEach(): Unit = {
    //cleanAllKafka()
    CollectorRegistry.defaultRegistry.clear()
    EmbeddedKafka.stop()
    EmbeddedKafka.stopZooKeeper()
    EmbeddedKafka.deleteTopics(List(Messages.jsonTopic, Messages.encodingTopic))
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

  def startKafka(bootstrapServers: String): FilterService = {

    val Injector = FakeSimpleInjector(bootstrapServers, Messages.jsonTopic)
    val consumer = Injector.get[DefaultFilterService]
    consumer.consumption.startPolling()
    consumer
  }

  "Missing redis connection" must {

    "should cause error messages" in {
      implicit val kafkaConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)
      val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort

      withRunningKafka {

        try {
          val embeddedRedisPid = getPidOfServiceUsingGivenPort(6379)
          proc("kill", "-9", embeddedRedisPid).call()
        } catch {
          case _: Throwable =>
        }

        val msgEnvelope = generateMessageEnvelope()

        startKafka(bootstrapServers)

        //publish message first time
        publishToKafka(Messages.jsonTopic, msgEnvelope)

        Thread.sleep(3000)

        consumeFirstMessageFrom[MessageEnvelope](Messages.encodingTopic).ubirchPacket.getUUID mustBe
          msgEnvelope.ubirchPacket.getUUID

        val cacheError1 = consumeFirstMessageFrom[FilterError](Messages.errorTopic)
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
        publishToKafka(Messages.jsonTopic, msgEnvelope)
        startKafka(bootstrapServers)
        val cacheError1 = consumeFirstMessageFrom[FilterError](Messages.errorTopic)
        cacheError1.exceptionName mustBe "NoCacheConnectionException"
        consumeFirstMessageFrom[MessageEnvelope](Messages.encodingTopic).ubirchPacket.getUUID mustBe
          msgEnvelope.ubirchPacket.getUUID

        //publish message second time (replay attack)
        publishToKafka(Messages.jsonTopic, msgEnvelope)

        val cacheError2 = consumeFirstMessageFrom[FilterError](Messages.errorTopic)
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
        publishToKafka(Messages.jsonTopic, msgEnvelope1)
        startKafka(bootstrapServers)
        consumeFirstMessageFrom[MessageEnvelope](Messages.encodingTopic).ubirchPacket.getUUID mustBe
          msgEnvelope1.ubirchPacket.getUUID
        val cacheError1 = consumeFirstMessageFrom[FilterError](Messages.errorTopic)
        cacheError1.exceptionName mustBe "NoCacheConnectionException"
        redis = new RedisServer(6379)
        redis.start()
        Thread.sleep(8000)

        //publish second message time
        val msgEnvelope2 = generateMessageEnvelope()
        publishToKafka(Messages.jsonTopic, msgEnvelope2)

        assertThrows[TimeoutException] {
          consumeNumberMessagesFrom[FilterError](Messages.errorTopic, 2)
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
