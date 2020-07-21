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

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.TimeoutException
import java.util.Base64

import com.fasterxml.jackson.core.JsonParseException
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.filter.model.cache.{ Cache, NoCacheConnectionException }
import com.ubirch.filter.model.{ FilterError, Rejection, Values }
import com.ubirch.filter.model.eventlog.Finder
import com.ubirch.filter.ConfPaths.{ ConsumerConfPaths, FilterConfPaths, ProducerConfPaths }
import com.ubirch.filter.services.Lifecycle
import com.ubirch.kafka.MessageEnvelope
import com.ubirch.kafka._
import com.ubirch.kafka.express.ExpressKafka
import com.ubirch.kafka.util.Exceptions.NeedForPauseException
import com.ubirch.protocol.ProtocolMessage
import javax.inject.{ Inject, Singleton }
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{ ProducerRecord, RecordMetadata }
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization
import org.apache.kafka.common.serialization._
import org.json4s._
import org.json4s.jackson.JsonMethods.parse
import org.msgpack.core.MessagePack

import scala.collection.immutable
import scala.collection.JavaConverters._
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Success

case class ProcessingData(cr: ConsumerRecord[String, String], upp: ProtocolMessage) {
  def payloadHash: Array[Byte] = upp.getPayload.asText().getBytes(StandardCharsets.UTF_8)
  def payloadString: String = upp.getPayload.asText()
}

trait FilterService {

  /**
    * Method that extracts the message envelope from the incoming consumer record
    * (kafka message) and publishes and logs error messages in case of failure.
    *
    * @param cr The current consumer record to be checked.
    * @return Option of message envelope if (successfully) parsed from JSON.
    */
  def extractData(cr: ConsumerRecord[String, String]): Option[MessageEnvelope]

  /**
    * Method that checks if the hash/payload has been processed earlier of the event-log system
    * and publishes and logs error messages in case of failure. In case the cassandra connection is
    * down an exception is thrown to make the underlying Kafka app wait before continuing with
    * processing the same messages once more.
    *
    * @param data    The data to become processed.
    * @throws NeedForPauseException to communicate the underlying app to pause the processing
    *                               of further messages.
    * @return Returns the HTTP response.
    */
  @throws[NeedForPauseException]
  def makeVerificationLookup(data: ProcessingData): Future[Option[String]]

  /**
    * Method that checks the cache regarding earlier processing of a message with the same
    * hash/payload and publishes and logs error messages in case of failure.
    *
    * @param data The data to become processed.
    * @return A boolean if the hash/payload has been already processed once or not.
    */
  def cacheContainsHash(data: ProcessingData): Option[String]

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
    *
    * @param rejectionMessage The rejection message defining if attack recognised by cache or lookup service.
    */
  def reactOnReplayAttack(cr: ConsumerRecord[String, String], rejectionMessage: String): Future[Option[RecordMetadata]]

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
  def pauseKafkaConsumption(errorMessage: String, cr: ConsumerRecord[String, String], ex: Throwable, mayBeDuration: FiniteDuration): Nothing
}

abstract class AbstractFilterService(cache: Cache, finder: Finder, config: Config, lifecycle: Lifecycle) extends FilterService with ExpressKafka[String, String, Unit] with LazyLogging
  with ConsumerConfPaths with ProducerConfPaths with FilterConfPaths {

  override val prefix: String = "Ubirch"

  override val maxTimeAggregationSeconds: Long = 180
  override val consumerReconnectBackoffMsConfig: Long = config.getLong(RECONNECT_BACKOFF_MS_CONFIG)
  override val consumerReconnectBackoffMaxMsConfig: Long = config.getLong(RECONNECT_BACKOFF_MAX_MS_CONFIG)
  override val consumerTopics: Set[String] = config.getString(CONSUMER_TOPICS).split(", ").toSet
  override val consumerGroupId: String = config.getString(GROUP_ID)
  override val consumerMaxPollRecords: Int = config.getInt(MAX_POOL_RECORDS)
  override val consumerGracefulTimeout: Int = config.getInt(GRACEFUL_TIMEOUT)
  override def consumerBootstrapServers: String = config.getString(CONSUMER_BOOTSTRAP_SERVERS)

  override val producerBootstrapServers: String = config.getString(PRODUCER_BOOTSTRAP_SERVERS)
  val producerErrorTopic: String = config.getString(ERROR_TOPIC)
  val producerForwardTopic: String = config.getString(FORWARD_TOPIC)
  val producerRejectionTopic: String = config.getString(REJECTION_TOPIC)

  override val lingerMs: Int = config.getInt(LINGER_MS)

  override val metricsSubNamespace: String = config.getString(METRICS_SUB_NAMESPACE)
  override val keySerializer: serialization.Serializer[String] = new StringSerializer
  override val valueSerializer: serialization.Serializer[String] = new StringSerializer
  override val keyDeserializer: Deserializer[String] = new StringDeserializer
  override val valueDeserializer: Deserializer[String] = new StringDeserializer

  private val ubirchEnvironment = config.getString(ENVIRONMENT)
  val filterStateActive: Boolean = config.getBoolean(FILTER_STATE)

  implicit val formats: Formats = com.ubirch.kafka.formats
  val msgPackConfig = new MessagePack.PackerConfig().withStr8FormatSupport(false)
  /**
    * Method that processes all consumer records of the incoming batch (Kafka message).
    */
  override val process: Process = { crs =>

    val futureResponse: immutable.Seq[Future[Option[RecordMetadata]]] = crs.map { cr =>
      logger.debug("consumer record received with key: " + cr.requestIdHeader().orNull)

      extractData(cr).map { msgEnvelope: MessageEnvelope =>
        msgEnvelope.ubirchPacket.toString
        val data = ProcessingData(cr, msgEnvelope.ubirchPacket)

        if (!filterStateActive && ubirchEnvironment != Values.PRODUCTION_NAME) {
          forwardUPP(data)
        } else {
          cacheContainsHash(data) match {
            case Some(_) =>
              reactOnReplayAttack(cr, Values.FOUND_IN_CACHE_MESSAGE)
            case None =>
              makeVerificationLookup(data).flatMap {
                case Some(_) =>
                  logger.debug("Found a match in cassandra, launching reactOnReplayAttack")
                  reactOnReplayAttack(cr, Values.FOUND_IN_VERIFICATION_MESSAGE)
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

  def extractData(cr: ConsumerRecord[String, String]): Option[MessageEnvelope] = {
    try {
      val result = parse(cr.value()).extract[MessageEnvelope]
      Some(result)
    } catch {
      case ex: Exception =>
        publishErrorMessage(s"unable to parse consumer record with key: ${cr.requestIdHeader().orNull}.", cr, ex)
        None
    }
  }

  def cacheContainsHash(data: ProcessingData): Option[String] = {
    try {
      cache.get(data.payloadHash)
    } catch {
      case ex: Exception =>
        publishErrorMessage(s"unable to make cache lookup '${data.cr.requestIdHeader().orNull}'.", data.cr, ex)
        None
    }
  }

  def makeVerificationLookup(data: ProcessingData): Future[Option[String]] = {

    try {
      val trimmedValue = trimPayload(data.payloadString)
      finder.findUPP(trimmedValue)
    } catch {
      //Todo: Should I catch further Exceptions?
      case ex: TimeoutException =>
        val requestId = data.cr.requestIdHeader().orNull
        publishErrorMessage(s"cassandra timeout while verification lookup for $requestId.", data.cr, ex)
        throw NeedForPauseException(s"cassandra timeout while verification lookup for $requestId: ${ex.getMessage}", ex.getMessage, Some(2 seconds))
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
    try {
      val hash = data.upp.getPayload.asText().getBytes(StandardCharsets.UTF_8)
      cache.set(hash, b64(rawPacket(data.upp)))
    } catch {
      case ex: Exception =>
        publishErrorMessage(s"unable to add ${data.cr.requestIdHeader().orNull} to cache.", data.cr, ex)
    }
    val result = send(data.cr.toProducerRecord(topic = producerForwardTopic))
      .recoverWith { case _ => send(data.cr.toProducerRecord(topic = producerForwardTopic)) }
      .recoverWith { case ex =>
        pauseKafkaConsumption(s"kafka error, not able to publish  ${data.cr.requestIdHeader().orNull} to $producerForwardTopic", data.cr, ex, 2 seconds)
      }
    result.onComplete {
      case Success(_) => logger.info("successfully forwarded consumer record with key: {}", data.cr.requestIdHeader().orNull)
      case _ =>
    }
    result.map { x => Some(x) }
  }

  protected def send(producerRecord: ProducerRecord[String, String]): Future[RecordMetadata] = production.send(producerRecord)

  /**
    * Helper method that generates the producer record that will be sent when a replay attack is detected
    * @param cr The consumerRecord on which to act
    * @param rejectionMessage why the message was rejected
    * @return a producer record with the correct HTTP headers and value
    */
  def generateReplayAttackProducerRecord(cr: ConsumerRecord[String, String], rejectionMessage: String): ProducerRecord[String, String] = {
    cr.toProducerRecord(
      topic = producerRejectionTopic,
      headers = (cr.headers().toArray :+ new RecordHeader(Values.HTTP_STATUS_CODE_HEADER, Values.HTTP_STATUS_CODE_REJECTION_ERROR.getBytes(UTF_8))).toIterable.asJava,
      value = Rejection(cr.requestIdHeader().orNull, rejectionMessage, Values.REPLAY_ATTACK_NAME).toString
    )
  }

  def reactOnReplayAttack(cr: ConsumerRecord[String, String], rejectionMessage: String): Future[Option[RecordMetadata]] = {
    val producerRecordToSend: ProducerRecord[String, String] = generateReplayAttackProducerRecord(cr, rejectionMessage)
    send(producerRecordToSend)
      .recoverWith {
        case _ => send(producerRecordToSend)
      }.recoverWith { case ex: Exception =>
        pauseKafkaConsumption(s"kafka error: ${producerRecordToSend.value()} could not be send to topic $producerRejectionTopic", cr, ex, 2 seconds)
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
  private def publishErrorMessage(
      errorMessage: String,
      cr: ConsumerRecord[String, String],
      ex: Throwable
  ): Future[Any] = {

    logger.error(errorMessage, ex.getMessage, ex)
    val producerRecordToSend = cr.toProducerRecord(
      topic = producerErrorTopic,
      value = FilterError(cr.requestIdHeader().orNull, errorMessage, ex.getClass.getSimpleName, cr.value().replace("\"", "\\\"")).toString
    )
    send(producerRecordToSend)
      .recover { case _ => logger.error(s"failure publishing to error topic: $errorMessage") }
  }

  def pauseKafkaConsumption(errorMessage: String, cr: ConsumerRecord[String, String], ex: Throwable, mayBeDuration: FiniteDuration): Nothing = {
    publishErrorMessage(errorMessage, cr, ex)
    logger.error("throwing NeedForPauseException to pause kafka message consumption")
    throw NeedForPauseException(errorMessage, ex.getMessage, Some(mayBeDuration))
  }

  private def rawPacket(upp: ProtocolMessage): Array[Byte] = {
    val out = new ByteArrayOutputStream(255)
    val packer = msgPackConfig.newPacker(out)

    if (upp.getSigned != null) { packer.writePayload(upp.getSigned) }
    packer.packBinaryHeader(upp.getSignature.length)
    packer.writePayload(upp.getSignature)
    packer.flush()
    packer.close()

    out.toByteArray
  }

  private def b64(x: Array[Byte]): String = if (x != null) Base64.getEncoder.encodeToString(x) else null

  lifecycle.addStopHook { () =>
    logger.info("Shutting down kafka")
    Future.successful(consumption.shutdown(consumerGracefulTimeout, java.util.concurrent.TimeUnit.SECONDS))
  }
}

/**
  * * This service is responsible to check incoming messages for any suspicious
  * behaviours as for example replay attacks. Till now, this is the only check being done.
  * It processes Kafka messages, making first a lookup in it's own cache to see if a message
  * with the same hash/payload has been send already, if the cache is down or nothing was found,
  * a ubirch database is questioned if the hash/payload has already been processed by the
  * event-log. Only if no replay attack was found the message is forwarded to the event-log system.
  *
  * @param cache The cache used to check if a message has already been received before.
  * @param finder The finder used to check if a message has already been received before in the event log in case
  *               the cache is down
  * @param config The config file containing the configuration needed for the service
  * @author ${user.name}
  */
@Singleton
class DefaultFilterService @Inject() (cache: Cache, finder: Finder, config: Config, lifecycle: Lifecycle)(implicit val ec: ExecutionContext) extends AbstractFilterService(cache, finder, config, lifecycle) {

}
