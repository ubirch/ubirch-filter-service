package com.ubirch.filter.model.eventlog

import java.util.Date

import org.json4s.JValue

/**
  * EventLogBase that conforms a packet for the event T.
  *
  * @tparam T Represents the Type of the Event.
  */

trait EventLogBase[T] {

  val headers: Headers
  val id: String
  val customerId: String
  val serviceClass: String
  val category: String
  val eventTime: Date
  val event: T
  val signature: String
  val nonce: String
  val lookupKeys: Seq[LookupKey]

  def withCustomerId(customerId: String): EventLog

  def withCategory(category: String): EventLogBase[T]

  def withServiceClass(serviceClass: String): EventLogBase[T]

  def withEventTime(eventTime: Date): EventLogBase[T]

  def withNewId(id: String): EventLogBase[T]

  def withSignature(signature: String): EventLogBase[T]

  def withNonce(nonce: String): EventLogBase[T]

}

/**
  * An EventLog whose Type T is JValue
  */
trait JValueEventLog extends EventLogBase[JValue]

/**
  * Concrete type for the EventLogBase whose type T is JValue
  * @param headers Represents a data structure that can contain key-value properties
  * @param id            String that identifies the EventLog. It can be a hash or a UUID or anything unique
  * @param customerId    Represents an id for a customer id.
  * @param serviceClass  Represents the name from where the log comes.
  *                     E.G: The name of the class.
  * @param category      Represents the category for the event. This is useful for
  *                      adding layers of description to the event.
  * @param event         Represents the event that is to be recorded.
  * @param eventTime     Represents the time when the event log was created.
  * @param signature     Represents the signature for the event log.
  * @param nonce         Represents a value that can be used to calculate the hash of the event.
  */
case class EventLog(
                     headers: Headers,
                     id: String,
                     customerId: String,
                     serviceClass: String,
                     category: String,
                     event: JValue,
                     eventTime: Date,
                     signature: String,
                     nonce: String,
                     lookupKeys: Seq[LookupKey]
                   ) extends JValueEventLog {

  override def withNewId(id: String): EventLog = this.copy(id = id)

  override def withCustomerId(customerId: String): EventLog = this.copy(customerId = customerId)

  override def withCategory(category: String): EventLog = this.copy(category = category)

  override def withServiceClass(serviceClass: String): EventLog = this.copy(serviceClass = serviceClass)

  override def withEventTime(eventTime: Date): EventLog = this.copy(eventTime = eventTime)

  override def withSignature(signature: String): EventLog = this.copy(signature = signature)

  override def withNonce(nonce: String): EventLog = this.copy(nonce = nonce)

}
