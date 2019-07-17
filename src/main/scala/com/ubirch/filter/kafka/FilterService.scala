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
import java.util.UUID
import java.util.concurrent.TimeoutException

import com.fasterxml.jackson.core.JsonParseException
import com.softwaremill.sttp.{HttpURLConnectionBackend, _}
import com.ubirch.filter.cache.Cache
import com.ubirch.filter.model.{Error, Rejection}
import com.ubirch.filter.util.Messages
import com.ubirch.kafka.MessageEnvelope
import com.ubirch.kafka.express.ExpressKafkaApp
import com.ubirch.kafka.util.Exceptions.NeedForPauseException
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization
import org.apache.kafka.common.serialization._
import org.json4s._
import org.json4s.jackson.JsonMethods.parse

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
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

          if (cacheContainsHash(payload, cr)) {
            reactOnReplayAttack(cr, msgEnvelope, Messages.foundInCacheMsg)
          } else {
            makeVerificationLookup(payload).code match {
              case StatusCodes.Ok =>
                reactOnReplayAttack(cr, msgEnvelope, Messages.foundInVerificationMsg)
              case StatusCodes.NotFound =>
                forwardUPP(cr, payload)
              case status =>
                logger.error(s"verification service failure: http-status-code: $status for payload: $payload.")
                throw NeedForPauseException("error processing data by filter service", s"verification service failure: http-status-code: $status for payload: $payload.", Some(2 seconds))
            }
          }
        case None =>
      }
    }
  }

  def extractData(cr: ConsumerRecord[String, Array[Byte]]): Option[(MessageEnvelope, String)] = {
    try {
      parse(new ByteArrayInputStream(cr.value())).extractOpt[MessageEnvelope].collect {
        case envelope => (envelope, envelope.ubirchPacket.getPayload.asText())
      }
    } catch {
      case ex: JsonParseException =>
        publishErrorMessage(s"unable to parse consumer record with key: ${cr.key()}: ${ex.getMessage}", cr.toString, ex) //Todo: add cr
        None
      //Todo: should I remove generic Exception? I could only trigger JsonParseException
      case ex: Exception =>
        publishErrorMessage(s"unable to parse consumer record with key: ${cr.key()}: ${ex.getMessage}", cr.toString, ex) //Todo: add cr
        None
    }
  }

  def cacheContainsHash(payload: String, cr: ConsumerRecord[String, Array[Byte]]): Boolean = {
    try {
      cache.get(payload)
    } catch {
      case ex: Exception =>
        publishErrorMessage(s"unable to lookup '$payload': ${ex.getMessage}", "", ex) //Todo: add cr
        false
    }
  }

  def makeVerificationLookup(payload: String)(implicit backend: SttpBackend[Id, Nothing]): Id[Response[String]] = {
    try {
      sttp
        .body(payload)
        .headers(("Content-Type", "application/json"), ("Charset", "UTF-8"))
        .post(uri"https://verify.$ubirchEnvironment.ubirch.com/api/verify")
        .send()
    } catch {
      //Todo: Should I catch further Exceptions?
      case ex: TimeoutException =>
        publishErrorMessage(s"http timeout while verification lookup for $payload: ${ex.getMessage}", payload, ex)
        throw NeedForPauseException(s"http timeout while verification lookup for $payload: ${ex.getMessage}", ex.getMessage, Some(2 seconds))
    }
  }

  def forwardUPP(cr: ConsumerRecord[String, Array[Byte]], payload: String): Unit = {
    try
      cache.set(payload)
    catch {
      case ex: Exception =>
        publishErrorMessage(s"unable to add $payload to cache: ${ex.getMessage}", cr.toString, ex) //Todo: add cr
    }
    send(Messages.encodingTopic, cr.value())
      .recover { case _ => send(Messages.encodingTopic, cr.value()) }
      .recover { case ex =>
        publishErrorMessage(s"kafka error, not able to publish  ${cr.key()} to ${Messages.encodingTopic}", cr.toString, ex, 2 seconds)
      }
    logger.info("upp message successfully forwarded to encoder: " + payload)
  }

  def reactOnReplayAttack(cr: ConsumerRecord[String, Array[Byte]], msgEnvelope: MessageEnvelope, rejectionMessage: String): Unit = {
    implicit val rejectionFormats: DefaultFormats.type = DefaultFormats
    val rj: Rejection = Rejection(msgEnvelope.ubirchPacket.getUUID, rejectionMessage, Messages.replayAttackName)
    send(Messages.rejectionTopic, rj.toString.getBytes())
      .recover { case _ => send(Messages.rejectionTopic, rj.toString.getBytes()) }
      .recover { case ex: Exception =>
        publishErrorMessage(s"kafka error: ${rj.toString} could not be send to topic ${Messages.rejectionTopic}", cr.toString, ex, 2 seconds)
      }
    logger.info("replay attack has been detected and successfully published: " + rejectionMessage)
  }

  def publishErrorMessage(errorMessage: String, value: String, ex: Throwable): Future[Any] = {
    val key = UUID.randomUUID()
    logger.error(errorMessage, ex.getMessage, ex)
    send(Messages.errorTopic, Error(key, errorMessage, ex.getClass.getSimpleName, value).toString.getBytes)
      .recover { case _ => logger.info(s"failure publishing to error topic: $errorMessage") }
  }

  @throws[NeedForPauseException]
  def publishErrorMessage(errorMessage: String, value: String, ex: Throwable, mayBeDuration: FiniteDuration): Unit = {
    publishErrorMessage(errorMessage, value, ex)
    throw NeedForPauseException(errorMessage, ex.getMessage, Some(mayBeDuration))
  }


}
