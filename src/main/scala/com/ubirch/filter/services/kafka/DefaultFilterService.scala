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

import java.io.ByteArrayInputStream
import java.util.concurrent.TimeoutException

import com.fasterxml.jackson.core.JsonParseException
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.filter.cache.{Cache, NoCacheConnectionException}
import com.ubirch.filter.model.{FilterError, Rejection, Values}
import com.ubirch.filter.model.eventlog.{EventLogRow, Finder}
import com.ubirch.filter.util.Messages
import com.ubirch.filter.ConfPaths.{ConsumerConfPaths, FilterConfPaths, ProducerConfPaths}
import com.ubirch.kafka.MessageEnvelope
import com.ubirch.kafka.express.ExpressKafka
import com.ubirch.kafka.util.Exceptions.NeedForPauseException
import javax.inject.{Inject, Singleton}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization
import org.apache.kafka.common.serialization._
import org.json4s._
import org.json4s.jackson.JsonMethods.parse

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Success


case class ProcessingData(cr: ConsumerRecord[String, Array[Byte]], payload: String)

trait FilterService {

  /**
    * Method that extracts the message envelope from the incoming consumer record
    * (kafka message) and publishes and logs error messages in case of failure.
    *
    * @param cr The current consumer record to be checked.
    * @return Option of message envelope if (successfully) parsed from JSON.
    */
  def extractData(cr: ConsumerRecord[String, Array[Byte]]): Option[MessageEnvelope]

  /**
    * Method that checks if the hash/payload has been processed earlier of the event-log system
    * and publishes and logs error messages in case of failure. In case the lookup service is
    * down an exception is thrown to make the underlying Kafka app wait before continuing with
    * processing the same messages once more.
    *
    * @param data    The data to become processed.
    * @throws NeedForPauseException to communicate the underlying app to pause the processing
    *                               of further messages.
    * @return Returns the HTTP response.
    */
  @throws[NeedForPauseException]
  def makeVerificationLookup(data: ProcessingData): Future[Option[EventLogRow]]

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
  def cacheContainsHash(data: ProcessingData): Boolean

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
  def forwardUPP(data: ProcessingData): Future[Option[RecordMetadata]]

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
  def reactOnReplayAttack(cr: ConsumerRecord[String, Array[Byte]], msgEnvelope: MessageEnvelope, rejectionMessage: String): Future[Option[RecordMetadata]]

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
  def pauseKafkaConsumption(errorMessage: String, cr: ConsumerRecord[String, Array[Byte]], ex: Throwable, mayBeDuration: FiniteDuration): Nothing
}

abstract class AbstractFilterService(cache: Cache, finder: Finder, val config: Config) extends FilterService with ExpressKafka[String, Array[Byte], Unit] with LazyLogging {

  override val prefix: String = "Ubirch"

  override val maxTimeAggregationSeconds: Long = 180
  override val lingerMs: Int = config.getInt(ProducerConfPaths.LINGER_MS)
  override val consumerReconnectBackoffMsConfig: Long = config.getLong(ConsumerConfPaths.RECONNECT_BACKOFF_MS_CONFIG)
  override val consumerReconnectBackoffMaxMsConfig: Long = config.getLong(ConsumerConfPaths.RECONNECT_BACKOFF_MAX_MS_CONFIG)
  override val metricsSubNamespace: String = config.getString(ConsumerConfPaths.METRICS_SUB_NAMESPACE)

  override val producerBootstrapServers: String = config.getString(ProducerConfPaths.BOOTSTRAP_SERVERS)
  override val keySerializer: serialization.Serializer[String] = new StringSerializer
  override val valueSerializer: serialization.Serializer[Array[Byte]] = new ByteArraySerializer
  override val consumerTopics: Set[String] = config.getString(ConsumerConfPaths.TOPICS).split(", ").toSet

  override def consumerBootstrapServers: String = config.getString(ConsumerConfPaths.BOOTSTRAP_SERVERS)

  override val consumerGroupId: String = config.getString(ConsumerConfPaths.GROUP_ID)
  override val consumerMaxPollRecords: Int = config.getInt(ConsumerConfPaths.MAX_POOL_RECORDS)
  override val consumerGracefulTimeout: Int = config.getInt(ConsumerConfPaths.GRACEFUL_TIMEOUT)
  override val keyDeserializer: Deserializer[String] = new StringDeserializer
  override val valueDeserializer: Deserializer[Array[Byte]] = new ByteArrayDeserializer

  implicit val formats: Formats = com.ubirch.kafka.formats
  private val ubirchEnvironment = config.getString(FilterConfPaths.ENVIRONMENT)
  val producerErrorTopic: String = config.getString(ProducerConfPaths.ERROR_TOPICS)

  val filterStateActive: Boolean = config.getBoolean(FilterConfPaths.FILTER_STATE)

  /**
    * Method that processes all consumer records of the incoming batch (Kafka message).
    */
  override val process: Process = { crs =>

    val futureResponse: immutable.Seq[Future[Option[RecordMetadata]]] = crs.map { cr =>
      logger.debug("consumer record received with key: " + cr.key())

      extractData(cr).map { msgEnvelope: MessageEnvelope =>

        val data = ProcessingData(cr, msgEnvelope.ubirchPacket.getPayload.toString)

        if (!filterStateActive && ubirchEnvironment != Values.PRODUCTION_NAME) {
          forwardUPP(data)
        } else {
          if (cacheContainsHash(data)) {
            reactOnReplayAttack(cr, msgEnvelope, Messages.foundInCacheMsg)
          } else {
            makeVerificationLookup(data).flatMap {
              case Some(_) =>
                logger.debug("Found a match in cassandra, launching reactOnReplayAttack")
                reactOnReplayAttack(cr, msgEnvelope, Messages.foundInVerificationMsg)
              case None =>
                logger.debug("Found no match in cassandra")
                forwardUPP(data)
            }
          }
        }
      }.getOrElse(Future.successful(None))

    }
    Future.sequence(futureResponse).map(_ => ())

  }


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

  def makeVerificationLookup(data: ProcessingData): Future[Option[EventLogRow]] = {

    try {
      val trimmedValue = trimPayload(data.payload)
      finder.findUPP(trimmedValue)
    } catch {
      //Todo: Should I catch further Exceptions?
      case ex: TimeoutException =>
        publishErrorMessage(s"http timeout while verification lookup for ${data.cr.key()}.", data.cr, ex)
        throw NeedForPauseException(s"http timeout while verification lookup for ${data.cr.key()}: ${ex.getMessage}", ex.getMessage, Some(2 seconds))
    }
  }

  /**
  * Sometimes the payload contains " at the beginning and the end of the payload, which makes cassandra go nuts and prevent
    * the value from being found
    * @param payload the payload to (eventually) trim
    * @return a trimmed string on which the first and last char will not be "
    */
  private def trimPayload(payload: String) = {
    if (payload.length > 2) {
      if (payload.head == '\"' && payload.endsWith("\"")) {
        payload.substring(1, payload.length - 1)
      } else payload
    } else payload
  }

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

  def pauseKafkaConsumption(errorMessage: String, cr: ConsumerRecord[String, Array[Byte]], ex: Throwable, mayBeDuration: FiniteDuration): Nothing = {
    publishErrorMessage(errorMessage, cr, ex)
    logger.error("throwing NeedForPauseException to pause kafka message consumption")
    throw NeedForPauseException(errorMessage, ex.getMessage, Some(mayBeDuration))
  }
}

/** * This service is responsible to check incoming messages for any suspicious
 * behaviours as for example replay attacks. Till now, this is the only check being done.
 * It processes Kafka messages, making first a lookup in it's own cache to see if a message
 * with the same hash/payload has been send already, if the cache is down or nothing was found,
 * a ubirch lookup service is questioned if the hash/payload has already been processed by the
 * event-log. Only if no replay attack was found the message is forwarded to the event-log system.
 *
 * @param cache The cache used to check if a message has already been received before.
 * @param finder The finder used to check if a message has already been received before in the event log in case
 *               the cache is down
 * @param config The config file containing the configuration needed for the service
 * @author ${user.name}
 */
@Singleton
class DefaultFilterService @Inject()(cache: Cache, finder: Finder, config: Config)(implicit val ec: ExecutionContext) extends AbstractFilterService(cache, finder, config) {

}
