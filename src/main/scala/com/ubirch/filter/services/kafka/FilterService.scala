package com.ubirch.filter.services.kafka

import com.ubirch.filter.model.{FilterReaction, ProcessingData}
import com.ubirch.kafka.MessageEnvelope
import com.ubirch.kafka.util.Exceptions.NeedForPauseException
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.json4s.Formats
import org.json4s.ext.JavaTypesSerializers

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

object FilterService {
  implicit val formats: Formats = com.ubirch.kafka.formats ++ JavaTypesSerializers.all
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
    * @param data The data to become processed.
    * @throws NeedForPauseException to communicate the underlying app to pause the processing
    *                               of further messages.
    * @return Returns the HTTP response.
    */
  @throws[NeedForPauseException]
  def makeVerificationLookup(data: ProcessingData): Future[Option[String]]

  /**
    * Method that checks the cache regarding earlier processing of a message with the same
    * hash/payload and decides on how to proceed with the incoming upp depending on the type
    * of the new and the cached upp. It also publishes error messages in case of failures.
    *
    * @param data The data to become processed.
    * @return A boolean if the hash/payload has been already processed once or not.
    */
  protected def decideReactionBasedOnCache(data: ProcessingData): Future[FilterReaction]

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
    * @param rejectionMessage The rejection message defining if attack recognised by cache or lookup service.
    */
  def reactOnReplayAttack(data: ProcessingData, cr: ConsumerRecord[String, String], rejectionMessage: String): Future[Option[RecordMetadata]]

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
