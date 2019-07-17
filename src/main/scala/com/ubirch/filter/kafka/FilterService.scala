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

import java.io.ByteArrayInputStream

import com.fasterxml.jackson.core.JsonParseException
import com.softwaremill.sttp.{HttpURLConnectionBackend, _}
import com.ubirch.filter.cache.Cache
import com.ubirch.filter.model.Rejection
import com.ubirch.filter.util.Messages
import com.ubirch.kafka.MessageEnvelope
import com.ubirch.kafka.express.ExpressKafkaApp
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization
import org.apache.kafka.common.serialization._
import org.json4s._
import org.json4s.jackson.JsonMethods.parse

import scala.language.postfixOps

class FilterService(cache: Cache) extends ExpressKafkaApp[String, Array[Byte]] {

  override val producerBootstrapServers: String = conf.getString("kafkaApi.kafkaProducer.bootstrapServers")
  override val keySerializer: serialization.Serializer[String] = new StringSerializer
  override val valueSerializer: serialization.Serializer[Array[Byte]] = new ByteArraySerializer
  override val consumerTopics: Set[String] = conf.getString("kafkaApi.kafkaConsumer.topic").split(", ").toSet
  val producerErrorTopic: String = conf.getString("kafkaApi.kafkaConsumer.errorTopic")

  override def consumerBootstrapServers: String = conf.getString("kafkaApi.kafkaConsumer.bootstrapServers")

  override val consumerGroupId: String = conf.getString("kafkaApi.kafkaConsumer.groupId")
  override val consumerMaxPollRecords: Int = conf.getInt("kafkaApi.kafkaConsumer.maxPoolRecords")
  override val consumerGracefulTimeout: Int = conf.getInt("kafkaApi.kafkaConsumer.gracefulTimeout")
  override val keyDeserializer: Deserializer[String] = new StringDeserializer
  override val valueDeserializer: Deserializer[Array[Byte]] = new ByteArrayDeserializer
  private val ubirchEnvironment = conf.getString("ubirch.environment")

  implicit val formats: Formats = com.ubirch.kafka.formats
  implicit val backend: SttpBackend[Id, Nothing] = HttpURLConnectionBackend()

  override def process(consumerRecords: Vector[ConsumerRecord[String, Array[Byte]]]): Unit = {
    consumerRecords.foreach { cr =>

      extractData(cr) match {

        case Some((msgEnvelope, payload)) =>

          if (cacheContainsHash(payload)) {
            reactOnReplayAttack(cr, msgEnvelope, Messages.foundInCacheMsg)
          } else {
            makeCassandraLookup(payload).code match {
              case StatusCodes.Ok =>
                reactOnReplayAttack(cr, msgEnvelope, Messages.foundInCassandraMsg)
              case _ =>
                forwardUPP(cr, payload)
            }
          }
        case None =>
      }
    }
  }

  def extractData(cr: ConsumerRecord[String, Array[Byte]]): Option[(MessageEnvelope, String)] = {
    try {
      parse(new ByteArrayInputStream(cr.value())).extractOpt[MessageEnvelope].collect {
        case envelope =>
          logger.info("Everything goes well.")
          (envelope, envelope.ubirchPacket.getPayload.asText())
      }
    } catch {
      case jsonEx: JsonParseException =>
        logger.error(s"unable to parse consumer record with key: ${cr.key()}: ${jsonEx.getMessage}", jsonEx)
        None
      //Todo: should I remove generic Exception? I could only trigger JsonParseException
      case ex: Exception =>
        logger.error(s"unable to parse consumer record with key: ${cr.key()}: ${ex.getMessage}", ex)
        None
    }
  }

  def cacheContainsHash(payload: String): Boolean = {
    try {
      cache.get(payload)
    } catch {
      case ex: Exception =>
        logger.error(s"unable to lookup '$payload': ${ex.getMessage}", ex)
        false
    }
  }

  def makeCassandraLookup(payload: String)(implicit backend: SttpBackend[Id, Nothing]): Id[Response[String]] = {
    //    try {
    sttp
      .body(payload)
      .headers(("Content-Type", "application/json"), ("Charset", "UTF-8"))
      .post(uri"https://verify.$ubirchEnvironment.ubirch.com/api/verify")
      .send()
    //    } catch {
    //      //probably
    //      case ex: TimeoutException =>
    //        logger.error(s"http timeout while cassandra lookup for $payload: ${ex.getMessage}", ex)
    //    }
  }

  def forwardUPP(cr: ConsumerRecord[String, Array[Byte]], payload: String): Any = {
    try {
      cache.set(payload)
    } catch {
      case ex: Exception =>
        logger.error(s"unable to add $payload to cache: ${ex.getMessage}", ex)
    }
    logger.info("UPP message forwarded to encoder: " + payload)
    try {
      send(Messages.encodingTopic, cr.value())
    } catch {
      case ex: Exception =>
        logger.info(s"kafka error: $payload could not be send to topic ${Messages.encodingTopic}: ${ex.getMessage}", ex)
    }

  }

  def reactOnReplayAttack(cr: ConsumerRecord[String, Array[Byte]], msgEnvelope: MessageEnvelope, rejectionMessage: String): Any = {
    logger.info("replay attack has been detected: " + rejectionMessage)
    implicit val rejectionFormats: DefaultFormats.type = DefaultFormats
    val rj: Rejection = Rejection(msgEnvelope.ubirchPacket.getUUID, rejectionMessage, "replayAttack")
    try {
      send(Messages.rejectionTopic, rj.toString.getBytes())
    } catch {
      case ex: Exception =>
        logger.info(s"kafka error: ${rj.toString} could not be send to topic ${Messages.rejectionTopic}: ${ex.getMessage}", ex)
    }
  }

}
