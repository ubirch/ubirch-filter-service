package com.ubirch.filter.model.eventlog

import java.util.Date

import io.getquill.Embedded
import org.json4s.JValue

/**
  * Concrete type for the EventLogBase whose type T is JValue
  *
  * @param id            String that identifies the EventLog. It can be a hash or a UUID or anything unique
  * @param customerId    Represents an id for a customer id.
  * @param serviceClass  Represents the name from where the log comes.
  *                     E.G: The name of the class.
  * @param category      Represents the category for the event. This is useful for
  *                      adding layers of description to the event.
  * @param event         Represents the event that is to be recorded.
  * @param eventTime     Represents the time when the event log was created.
  * @param eventTimeInfo Represents the time of the event in an unfolded manner.
  *                      This is useful and needed for making cluster keys with
  *                      the time of the event possible. Helpers are provided
  *                      to support its creation from the eventTime.
  * @param signature     Represents the signature for the event log.
  * @param nonce         Represents a value that can be used to calculate the hash of the event.
  * @param status        Represents the status of the EventlogRow (at the moment only used for UPP categories.
  *                      If it has become disabled, it shouldn't be possible to verify the UPP). It's value might
  *                      be either NONE, Some(ENABLED) or Some(DISABLED). None is equivalent to Some(ENABLED).
  */

case class EventLogRow(
  id: String,
  customerId: String,
  serviceClass: String,
  category: String,
  event: JValue,
  eventTime: Date,
  eventTimeInfo: TimeInfo,
  signature: String,
  nonce: String,
  status: Option[String] = None
)

/**
  * This case class represents the the explicit values of the event time on the cassandra db.
  * They are explicitly shown to help have better clustering keys.
  * This class is an expansion of the event time date.
  * @param year Represents the year when the event took place
  * @param month Represents the month when the event took place
  * @param day Represents the day when the event took place
  * @param hour Represents the hour when the event took place
  * @param minute Represents the minutes when the event took place
  * @param second Represents the seconds when the event took place
  * @param milli Represents the milliseconds when the event took place
  */
case class TimeInfo(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int, milli: Int) extends Embedded
