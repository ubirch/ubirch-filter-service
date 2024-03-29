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

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.filter.ConfPaths.{ ConsumerConfPaths, FilterConfPaths, ProducerConfPaths }
import com.ubirch.filter.model.Values.{ UPP_TYPE_DELETE, UPP_TYPE_DISABLE, UPP_TYPE_ENABLE }
import com.ubirch.filter.model._
import com.ubirch.filter.model.cache.Cache
import com.ubirch.filter.model.eventlog.Finder
import com.ubirch.filter.services.Lifecycle
import com.ubirch.filter.util.ProtocolMessageUtils.{ base64Decoder, base64Encoder, msgPackDecoder, rawPacket }
import com.ubirch.kafka.express.ExpressKafka
import com.ubirch.kafka.util.Exceptions.NeedForPauseException
import com.ubirch.kafka.{ MessageEnvelope, RichAnyConsumerRecord, _ }
import monix.execution.Scheduler
import net.logstash.logback.argument.StructuredArguments.v
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{ ProducerRecord, RecordMetadata }
import org.apache.kafka.common.serialization
import org.apache.kafka.common.serialization._
import org.json4s._
import org.json4s.jackson.JsonMethods.parse

import java.util.concurrent.TimeoutException
import javax.inject.{ Inject, Singleton }
import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.language.{ implicitConversions, postfixOps }

/**
  * * This service is responsible to check incoming messages for any suspicious
  * behaviours as for example replay attacks. Till now, this is the only check being done.
  * It processes Kafka messages, making first a lookup in it's own cache to see if a message
  * with the same hash/payload has been send already, if the cache is down or nothing was found,
  * a ubirch database is questioned if the hash/payload has already been processed by the
  * event-log. Only if no replay attack was found the message is forwarded to the event-log system.
  *
  * @param cache  The cache used to check if a message has already been received before.
  * @param finder The finder used to check if a message has already been received before in the event log in case
  *               the cache is down
  * @param config The config file containing the configuration needed for the service
  * @author ${user.name}
  */
@Singleton
class DefaultFilterService @Inject() (cache: Cache, finder: Finder, config: Config, lifecycle: Lifecycle)(
  implicit val ec: ExecutionContext,
  val scheduler: Scheduler)
  extends AbstractFilterService(cache, finder, config, lifecycle)

abstract class AbstractFilterService(cache: Cache, finder: Finder, config: Config, lifecycle: Lifecycle)
  extends FilterService
  with ExpressKafka[String, String, Unit]
  with LazyLogging
  with ConsumerConfPaths
  with ProducerConfPaths
  with FilterConfPaths {

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

  final private val HARDWARE_ID_HEADER_KEY = "x-ubirch-hardware-id"

  implicit val formats: Formats = FilterService.formats

  /**
    * Method that processes all consumer records of the incoming batch (Kafka message).
    */
  override val process: Process = { crs =>
    val futureResponse: immutable.Seq[Future[Option[RecordMetadata]]] = crs.map { cr =>
      val hardwareId = cr.findHeader(HARDWARE_ID_HEADER_KEY).orNull
      val requestId = cr.requestIdHeader().orNull
      logger.debug(
        s"consumer record received from $hardwareId with key: $requestId",
        v("requestId", requestId),
        v("hardwareId", hardwareId))

      extractData(cr).map { msgEnvelope =>
        val data = ProcessingData(cr, msgEnvelope.ubirchPacket)

        if (!filterStateActive && ubirchEnvironment != Values.PRODUCTION_NAME) {
          forwardUPP(data)
        } else {

          decideReactionBasedOnCache(data).flatMap {
            case RejectUPP => reactOnReplayAttack(data, cr, Values.FOUND_IN_CACHE_MESSAGE)

            case ForwardUPP => forwardUPP(data)

            case InvestigateFurther => decideReactionOnCassandra(cr, data)
          }
        }
      }.getOrElse(Future.successful(None))
    }
    Future.sequence(futureResponse).map(_ => ())

  }

  private def decideReactionOnCassandra(
    cr: ConsumerRecord[String, String],
    data: ProcessingData
  ): Future[Option[RecordMetadata]] = {

    val (hardwareId, requestId) = retrieveIdsForStructuredLogs(data)

    makeVerificationLookup(data).flatMap { uppOpt =>
      data.pm.getHint match {
        case UPP_TYPE_DELETE | UPP_TYPE_ENABLE | UPP_TYPE_DISABLE =>
          if (uppOpt.isDefined) {
            logger.info(
              s"Forwarding msg of type ${data.pm.getHint} from device $hardwareId with requestId: $requestId as msg with same hash was found in cassandra",
              v("requestId", requestId),
              v("hardwareId", hardwareId)
            )
            forwardUPP(data)
          } else {
            logger.info(
              s"Rejecting msg of type ${data.pm.getHint} from device $hardwareId with requestId: $requestId as msg with same hash was NOT found in cassandra",
              v("requestId", requestId),
              v("hardwareId", hardwareId)
            )
            reactOnReplayAttack(data, cr, Values.NOT_FOUND_IN_VERIFICATION_MESSAGE)
          }
        case _ =>
          if (uppOpt.isEmpty) {
            logger.info(
              s"Forwarding msg of type ${data.pm.getHint} from device $hardwareId with requestId: $requestId as msg with same hash was NOT found in cassandra",
              v("requestId", requestId),
              v("hardwareId", hardwareId)
            )
            forwardUPP(data)
          } else {
            logger.info(
              s"Rejecting msg of type ${data.pm.getHint} from device $hardwareId with requestId: $requestId as msg with same hash was found in cassandra",
              v("requestId", requestId),
              v("hardwareId", hardwareId)
            )
            reactOnReplayAttack(data, cr, Values.FOUND_IN_VERIFICATION_MESSAGE)
          }
      }
    }
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

  def filterCacheContains(data: ProcessingData): Future[Option[String]] = {
    cache.getFromFilterCache(data.payloadHash).recover {
      case ex: Exception =>
        publishErrorMessage(s"unable to make cache lookup '${data.cr.requestIdHeader().orNull}'.", data.cr, ex)
        None
    }
  }

  protected[services] def decideReactionBasedOnCache(data: ProcessingData): Future[FilterReaction] = {

    val (hardwareId, requestId) = retrieveIdsForStructuredLogs(data)

    filterCacheContains(data).map {

      case Some(cachedUpp: String) =>
        val cachedHint = retrieveHintOfUpp(cachedUpp)

        def reject(msgType: String): FilterReaction = {
          log(s"Rejecting $msgType")
          RejectUPP
        }

        def forward(msgType: String): FilterReaction = {
          log(s"Forwarding $msgType")
          ForwardUPP
        }

        def log(reaction: String): Unit = {
          logger.info(
            s"$reaction msg of type ${data.pm.getHint} from device $hardwareId with requestId: $requestId as msg with same hash and type $cachedHint was found in cache",
            v("requestId", requestId),
            v("hardwareId", hardwareId)
          )
        }

        data.pm.getHint match {

          case UPP_TYPE_DELETE =>
            cachedHint match {
              case UPP_TYPE_DELETE => reject("delete")
              case _               => forward("delete")
            }
          case UPP_TYPE_ENABLE =>
            cachedHint match {
              case UPP_TYPE_ENABLE | UPP_TYPE_DELETE => reject("enable")
              case _                                 => forward("enable")
            }
          case UPP_TYPE_DISABLE =>
            cachedHint match {
              case UPP_TYPE_DISABLE | UPP_TYPE_DELETE => reject("disable")
              case _                                  => forward("disable")
            }
          case _ =>
            cachedHint match {
              case UPP_TYPE_DELETE => forward("default")
              case _               => reject("default")
            }
        }

      case None =>
        InvestigateFurther

    }.recover {
      case ex =>
        logger.error(
          s"something went wrong checking for hint/ type of cached upp when processing msg from device $hardwareId with requestId: $requestId ",
          v("requestId", requestId),
          v("hardwareId", hardwareId),
          ex
        )
        InvestigateFurther
    }
  }

  private def retrieveHintOfUpp(cachedUpp: String): Int = {
    val byteArray = base64Decoder.decode(cachedUpp)
    val pm = msgPackDecoder.decode(byteArray)
    pm.getHint
  }

  def makeVerificationLookup(data: ProcessingData): Future[Option[String]] = {

    val trimmedValue = trimPayload(data.payloadString)
    finder.findUPP(trimmedValue).map {
      case Some(string) =>
        Some(string)
      case None =>
        None
    }.recover {
      case ex: TimeoutException =>
        val requestId = data.cr.requestIdHeader().orNull
        publishErrorMessage(s"cassandra timeout while verification lookup for $requestId.", data.cr, ex)
        throw NeedForPauseException(
          s"cassandra timeout while verification lookup for $requestId: ${ex.getMessage}",
          ex.getMessage,
          Some(FiniteDuration(2, SECONDS)))
    }
  }

  /**
    * Sometimes the payload contains " at the beginning and the end of the payload, which makes cassandra go nuts and prevent
    * the value from being found
    *
    * @param payload the payload to (eventually) trim
    * @return a trimmed string on which the first and last char will not be "
    */
  private def trimPayload(payload: String): String = {
    if (payload.length > 2) {
      if (payload.head == '\"' && payload.endsWith("\"")) {
        payload.substring(1, payload.length - 1)
      } else payload
    } else payload
  }

  def forwardUPP(data: ProcessingData): Future[Option[RecordMetadata]] = {

    val r1 = addToFilterCache(data)
    val r2 = deleteFromOrAddToVerificationCache(data)
    val r3 = sendAndRetry(data.cr.toProducerRecord(topic = producerForwardTopic), data.cr)
    for {
      _ <- r1
      _ <- r2
      recordMetaData <- r3
    } yield {
      val (hardwareId, requestId) = retrieveIdsForStructuredLogs(data)
      logger.info(
        s"Successfully forwarded msg from $hardwareId with requestId: $requestId",
        v("requestId", requestId),
        v("hardwareId", hardwareId))
      Some(recordMetaData)
    }
  }

  /**
    * This method deletes the upp from or adds it to the verification cache,
    * depending on the hint/ type of the incoming UPP.
    *
    * @param data incoming upp
    */
  private[services] def deleteFromOrAddToVerificationCache(data: ProcessingData): Future[Any] = {
    data.pm.getHint match {
      case UPP_TYPE_DELETE | UPP_TYPE_DISABLE => deleteFromVerificationCache(data)
      case UPP_TYPE_ENABLE                    => Future.successful(())
      case _                                  => addToVerificationCache(data)
    }
  }

  /**
    * Helper method to encode upp (to base64 encoded bytes) and store it to the cache.
    */
  private[services] def addToVerificationCache(data: ProcessingData): Future[Any] = {
    try {
      val base64EncodedUpp = base64Encoder.encodeToString(rawPacket(data.pm))
      cache
        .setToVerificationCache(data.payloadHash, base64EncodedUpp)
        .recoverWith {
          case ex: Exception =>
            publishErrorMessage(s"unable to add value for hash ${data.payloadString} to cache.", data.cr, ex)
        }
    } catch {
      case ex: NullPointerException =>
        publishErrorMessage(s"unable to add value for hash ${data.payloadString} to cache.", data.cr, ex)
        Future.successful(())
    }
  }

  /**
    * Helper method to encode upp (to base64 encoded bytes) and store it to the cache.
    */
  private[services] def addToFilterCache(data: ProcessingData): Future[Any] = {
    try {
      val base64EncodedUpp = base64Encoder.encodeToString(rawPacket(data.pm))
      cache.setToFilterCache(data.payloadHash, base64EncodedUpp).recover {
        case ex: Exception =>
          publishErrorMessage(s"unable to add value for hash ${data.payloadString} to cache.", data.cr, ex)
      }
    } catch {
      case ex: NullPointerException =>
        publishErrorMessage(s"unable to add value for hash ${data.payloadString} to cache.", data.cr, ex)
        Future.successful(())
    }
  }

  private def deleteFromVerificationCache(data: ProcessingData): Future[Boolean] = {
    cache
      .deleteFromVerificationCache(data.payloadHash)
      .recover {
        case ex: Exception =>
          publishErrorMessage(s"unable to delete upp by hash ${data.payloadString} from cache.", data.cr, ex)
          false
      }
  }

  def reactOnReplayAttack(
    data: ProcessingData,
    cr: ConsumerRecord[String, String],
    rejectionMessage: String): Future[Option[RecordMetadata]] = {

    val r1 = addToFilterCache(data)
    val r2 = sendAndRetry(generateReplayAttackProducerRecord(cr, rejectionMessage), data.cr)
    for {
      _ <- r1
      recordMetaData <- r2
    } yield {
      Some(recordMetaData)
    }
  }

  /**
    * Helper method that generates the producer record that will be sent when a replay attack is detected
    *
    * @param cr               The consumerRecord on which to act
    * @param rejectionMessage why the message was rejected
    * @return a producer record with the correct HTTP headers and value
    */
  protected[services] def generateReplayAttackProducerRecord(
    cr: ConsumerRecord[String, String],
    rejectionMessage: String): ProducerRecord[String, String] = {
    val payload = Error(
      error = Values.REPLAY_ATTACK_NAME,
      causes = Seq(rejectionMessage),
      requestId = cr.requestIdHeader().orNull).toJson
    cr.toProducerRecord(producerRejectionTopic, payload)
      .withExtraHeaders(
        Values.HTTP_STATUS_CODE_HEADER -> Values.HTTP_STATUS_CODE_REJECTION_ERROR,
        Values.PREVIOUS_MICROSERVICE -> "Niomon-Filter"
      )
  }

  private def sendAndRetry(
    pr: ProducerRecord[String, String],
    cr: ConsumerRecord[String, String]): Future[RecordMetadata] = {
    send(pr)
      .recoverWith { case _ => send(pr) }
      .recoverWith {
        case ex =>
          pauseKafkaConsumption(
            s"kafka error: ${pr.value()} could not be send to topic ${pr.topic()}",
            cr,
            ex,
            FiniteDuration(2, SECONDS))
      }
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
    val payload = Error(
      error = ex.getClass.getSimpleName,
      causes = Seq(errorMessage),
      requestId = cr.requestIdHeader().orNull).toJson
    val producerRecordToSend = cr
      .toProducerRecord(producerErrorTopic, payload)
      .withExtraHeaders(Values.PREVIOUS_MICROSERVICE -> "Niomon-Filter")
    send(producerRecordToSend)
      .recover { case _ => logger.error(s"failure publishing to error topic: $errorMessage") }
  }

  def pauseKafkaConsumption(
    errorMessage: String,
    cr: ConsumerRecord[String, String],
    ex: Throwable,
    mayBeDuration: FiniteDuration): Nothing = {
    publishErrorMessage(errorMessage, cr, ex)
    logger.error("throwing NeedForPauseException to pause kafka message consumption")
    throw NeedForPauseException(errorMessage, ex.getMessage, Some(mayBeDuration))
  }

  private def retrieveIdsForStructuredLogs(data: ProcessingData): (String, String) = {
    val hardwareId = data.cr.findHeader(HARDWARE_ID_HEADER_KEY).getOrElse("(hardware_id_header missing)")
    val requestId = data.cr.requestIdHeader().orNull
    (hardwareId, requestId)
  }

  lifecycle.addStopHook { () =>
    logger.info("Shutting down kafka")
    Future.successful(consumption.shutdown(consumerGracefulTimeout, java.util.concurrent.TimeUnit.SECONDS))
  }
}
