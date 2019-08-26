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
import java.util.concurrent.TimeoutException

import com.fasterxml.jackson.core.JsonParseException
import com.softwaremill.sttp.{HttpURLConnectionBackend, _}
import com.ubirch.filter.cache.{Cache, NoCacheConnectionException}
import com.ubirch.filter.model.{FilterError, Rejection}
import com.ubirch.filter.util.Messages
import com.ubirch.kafka.MessageEnvelope
import com.ubirch.kafka.express.ExpressKafkaApp
import com.ubirch.kafka.util.Exceptions.NeedForPauseException
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization
import org.apache.kafka.common.serialization._
import org.json4s._
import org.json4s.jackson.JsonMethods.parse

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Success

case class ProcessingData(cr: ConsumerRecord[String, Array[Byte]], payload: String)

/** * This service is responsible to check incoming messages for any suspicious
 * behaviours as for example replay attacks. Till now, this is the only check being done.
 * It processes Kafka messages, making first a lookup in it's own cache to see if a message
 * with the same hash/payload has been send already, if the cache is down or nothing was found,
 * a ubirch lookup service is questioned if the hash/payload has already been processed by the
 * event-log. Only if no replay attack was found the message is forwarded to the event-log system.
 *
 * @param cache The cache used to check if a message has already been received before.
 * @author ${user.name}
 */
class FilterService(cache: Cache) extends ExpressKafkaApp[String, Array[Byte], Unit] {

  override val producerBootstrapServers: String = conf.getString("filterService.kafkaApi.kafkaProducer.bootstrapServers")
  override val keySerializer: serialization.Serializer[String] = new StringSerializer
  override val valueSerializer: serialization.Serializer[Array[Byte]] = new ByteArraySerializer
  override val consumerTopics: Set[String] = conf.getString("filterService.kafkaApi.kafkaConsumer.topic").split(", ").toSet
  val producerErrorTopic: String = conf.getString("filterService.kafkaApi.kafkaConsumer.errorTopic")

  val filterStateActive: Boolean = conf.getBoolean("filterService.stateActive")

  override def consumerBootstrapServers: String = conf.getString("filterService.kafkaApi.kafkaConsumer.bootstrapServers")

  override val consumerGroupId: String = conf.getString("filterService.kafkaApi.kafkaConsumer.groupId")
  override val consumerMaxPollRecords: Int = conf.getInt("filterService.kafkaApi.kafkaConsumer.maxPoolRecords")
  override val consumerGracefulTimeout: Int = conf.getInt("filterService.kafkaApi.kafkaConsumer.gracefulTimeout")
  override val keyDeserializer: Deserializer[String] = new StringDeserializer
  override val valueDeserializer: Deserializer[Array[Byte]] = new ByteArrayDeserializer
  private val ubirchEnvironment = conf.getString("filterService.verification.environment")

  implicit val formats: Formats = formats
  implicit val backend: SttpBackend[Id, Nothing] = HttpURLConnectionBackend()

  /**
   * Method that processes all consumer records of the incoming batch (Kafka message).
   *
   * @param consumerRecords an entry of the incoming Kafka message
   */

  override def process(consumerRecords: Vector[ConsumerRecord[String, Array[Byte]]]): Future[Unit] = {


    val futureResponse = consumerRecords.map { cr =>
      logger.debug("consumer record received with key: " + cr.key())

      extractData(cr).map { msgEnvelope =>

        val data = ProcessingData(cr, msgEnvelope.ubirchPacket.getPayload.toString)

        if (!filterStateActive) {
          forwardUPP(data)
        } else {
          if (cacheContainsHash(data)) {
            reactOnReplayAttack(cr, msgEnvelope, Messages.foundInCacheMsg)
          } else {
            makeVerificationLookup(data).code match {
              case StatusCodes.Ok =>
                reactOnReplayAttack(cr, msgEnvelope, Messages.foundInVerificationMsg)
              case StatusCodes.NotFound =>
                forwardUPP(data)
              case status =>
                logger.error(s"verification service failure: http-status-code: $status for payload: $data.payload.")
                Future.failed(NeedForPauseException("error processing data by filter service", s"verification service failure: http-status-code: $status for key: ${data.cr.key()}.", Some(2 seconds)))
            }
          }
        }
      }.getOrElse(Future.successful(None))

    }
    Future.sequence(futureResponse).map(_ => ())

  }

  /**
   * Method that extracts the message envelope from the incoming consumer record
   * (kafka message) and publishes and logs error messages in case of failure.
   *
   * @param cr The current consumer record to be checked.
   * @return Option of message envelope if (successfully) parsed from JSON.
   */
  def extractData(cr: ConsumerRecord[String, Array[Byte]]): Option[MessageEnvelope] = {
    try {
      val result = parse(new ByteArrayInputStream(cr.value())).extract[MessageEnvelope]
      Some(result)
    } catch {
      case ex: MappingException =>
        publishErrorMessage(s"unable to parse message envelope with key: ${cr.key()}.", cr, ex)
        None
      case ex: JsonParseException =>
        publishErrorMessage(s"unable to parse consumer record with key: ${cr.key()}.", cr, ex)
        None
      //Todo: should I remove generic Exception? I could only trigger JsonParseException
      case ex: Exception =>
        publishErrorMessage(s"unable to parse consumer record with key: ${cr.key()}.", cr, ex)
        None
    }
  }

  /**
   * Method that checks the cache regarding earlier processing of a message with the same{
   * *
   * implicit val rejectionFormats: DefaultFormats.type = DefaultFormats
   * val rj = Rejection(cr.key, rejectionMessage, Messages.replayAttackName)
   * send(Messages.rejectionTopic, rj.toString.getBytes())
   * .recoverWith {
   * case _ => send(Messages.rejectionTopic, rj.toString.getBytes())
   * }.recoverWith { case ex: Exception =>
   * pauseKafkaConsumption(s"kafka error: ${rj.toString} could not be send to topic ${Messages.rejectionTopic}", cr, ex, 2 seconds)
   * }.map { x => Some(x) }
   * }
   *
   * hash/payload and publishes and logs error messages in case of failure.
   *
   * @param data The data to become processed.
   * @return A boolean if the hash/payload has been already processed once or not.
   */
  def cacheContainsHash(data: ProcessingData): Boolean = {
    try {
      cache.get(data.payload)
    } catch {
      case ex: NoCacheConnectionException =>
        publishErrorMessage(s"unable to make cache lookup '${data.cr.key()}'.", data.cr, ex)
        false
      case ex: Exception =>
        publishErrorMessage(s"unable to make cache lookup '${data.cr.key()}'.", data.cr, ex)
        false
    }
  }

  /**
   * Method that checks if the hash/payload has been processed earlier of the event-log system
   * and publishes and logs error messages in case of failure. In case the lookup service is
   * down an exception is thrown to make the underlying Kafka app wait before continuing with
   * processing the same messages once more.
   *
   * @param data    The data to become processed.
   * @param backend The HTTP Client for sending the lookup request.
   * @throws NeedForPauseException to communicate the underlying app to pause the processing
   *                               of further messages.
   * @return Returns the HTTP response.
   */
  @throws[NeedForPauseException]
  def makeVerificationLookup(data: ProcessingData)(implicit backend: SttpBackend[Id, Nothing]): Id[Response[String]] = {
    try {
      sttp
        .body(data.payload)
        .headers(("Content-Type", "application/json"), ("Charset", "UTF-8"))
        .post(uri"https://verify.$ubirchEnvironment.ubirch.com/api/verify")
        .send()
    } catch {
      //Todo: Should I catch further Exceptions?
      case ex: TimeoutException =>
        publishErrorMessage(s"http timeout while verification lookup for ${data.cr.key()}.", data.cr, ex)
        throw NeedForPauseException(s"http timeout while verification lookup for ${data.cr.key()}: ${ex.getMessage}", ex.getMessage, Some(2 seconds))
    }
  }

  /**
   * Method that forwards the incoming consumer record via Kafka in case no replay
   * attack has been found and publishes and logs error messages in case of failure.
   * In case publishing the message is failing an exception is thrown to make the underlying
   * Kafka app wait before continuing with processing the same messages once more.
   *
   * @param data The data to become processed.
   * @throws NeedForPauseException to communicate the underlying app to pause the processing
   *                               of further messages.
   */
  @throws[NeedForPauseException]
  def forwardUPP(data: ProcessingData): Future[Option[RecordMetadata]] = {
    try
      cache.set(data.payload)
    catch {
      case ex: NoCacheConnectionException =>
        publishErrorMessage(s"unable to add ${data.cr.key()} to cache", data.cr, ex)
      case ex: Exception =>
        publishErrorMessage(s"unable to add ${data.cr.key()} to cache.", data.cr, ex)
    }
    val result = send(Messages.encodingTopic, data.cr.value())
      .recoverWith { case _ => send(Messages.encodingTopic, data.cr.value()) }
      .recoverWith { case ex =>
        pauseKafkaConsumption(s"kafka error, not able to publish  ${data.cr.key()} to ${Messages.encodingTopic}", data.cr, ex, 2 seconds)
      }
    result.onComplete {
      case Success(_) => logger.info("successfully forwarded consumer record with key: {}", data.cr.key())
      case _ =>
    }
    result.map { x => Some(x) }
  }

  /**
   * Method that reacts on a replay attack by logging and publishing a rejection message
   * via Kafka and publishes and logs error messages in case of failure.CIn case publishing
   * the message is failing an exception is thrown to make the underlyingCKafka app wait
   * before continuing with processing the same messages once more.
   *
   * @param cr               The consumer record of the replay attack.
   * @param msgEnvelope      The message envelope of the replay attack.
   * @param rejectionMessage The rejection message defining if attack recognised by cache or lookup service.
   */
  def reactOnReplayAttack(cr: ConsumerRecord[String, Array[Byte]], msgEnvelope: MessageEnvelope, rejectionMessage: String): Future[Option[RecordMetadata]] = {

    implicit val rejectionFormats: DefaultFormats.type = DefaultFormats
    val rj = Rejection(cr.key, rejectionMessage, Messages.replayAttackName)
    send(Messages.rejectionTopic, rj.toString.getBytes())
      .recoverWith {
        case _ => send(Messages.rejectionTopic, rj.toString.getBytes())
      }.recoverWith { case ex: Exception =>
      pauseKafkaConsumption(s"kafka error: ${rj.toString} could not be send to topic ${Messages.rejectionTopic}", cr, ex, 2 seconds)
    }.map { x => Some(x) }
  }

  /**
   * A helper function for logging and publishing error messages.
   *
   * @param errorMessage A message informing about the error.
   * @param cr           The consumer record being processed while error happens.
   * @param ex           The exception being thrown.
   * @return
   */
  private def publishErrorMessage(errorMessage: String,
                                  cr: ConsumerRecord[String, Array[Byte]],
                                  ex: Throwable): Future[Any] = {

    logger.error(errorMessage, ex.getMessage, ex)
    send(Messages.errorTopic, FilterError(cr.key(), errorMessage, ex.getClass.getSimpleName, cr.value().toString).toString.getBytes)
      .recover { case _ => logger.error(s"failure publishing to error topic: $errorMessage") }
  }

  /**
   * Method that throws an exception in case the service cannot execute it's functionality properly
   * to make the underlying Kafka app wait with processing further messages. It also publishes and
   * logs error messages.
   *
   * @param errorMessage  A message informing about the error.
   * @param cr            The consumer record being processed while error happens.
   * @param ex            The exception being thrown.
   * @param mayBeDuration The duration before teh underlying Kafka app starts consuming the
   *                      same and other messages again.
   * @throws NeedForPauseException to communicate the underlying app to pause the processing
   *                               of further messages.
   */
  @throws[NeedForPauseException]
  def pauseKafkaConsumption(errorMessage: String, cr: ConsumerRecord[String, Array[Byte]], ex: Throwable, mayBeDuration: FiniteDuration): Nothing = {
    publishErrorMessage(errorMessage, cr, ex)
    logger.error("throwing NeedForPauseException to pause kafka message consumption")
    throw NeedForPauseException(errorMessage, ex.getMessage, Some(mayBeDuration))
  }

}
