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

package com.ubirch.filter.model

import java.io.ByteArrayInputStream
import java.util.{Date, UUID}

import org.apache.kafka.common.serialization.Deserializer
import org.json4s.{DefaultFormats, Formats}
import org.json4s.ext.JavaTypesSerializers
import org.json4s.jackson.Serialization.read

/**
  * Represents the error that is eventually published to Kafka.
  *
  * After trying to process/store the events, there's the possibility of getting an error.
  * This type is used to represent the error generated.
  *
  * @param id            represents the id of the incoming event.
  *                      If the id is not possible to read from the event data, it is automatically generated.
  * @param message       represents an informative rejection message.
  * @param rejectionName represents the name of the exception. E.g ParsingIntoEventLogException.
  * @param value         represent the event value.
  *                      It can be empty if a EmptyValueException was thrown or if the exception is not known
  *                      It can be a malformed event if a ParsingIntoEventLogException was thrown
  *                      It can be the well-formed event if a StoringIntoEventLogException was thrown
  * @param rejectionTime represents the time when the recection occurred
  * @param serviceName   represents the name of the service. By default, we use, event-log-service.
  */
case class Rejection(
                      id: UUID,
                      message: String,
                      rejectionName: String,
                      value: String = "empty",
                      rejectionTime: Option[Date] = Some(new java.util.Date()), //Todo: Is only parsed automatically when held as Option :/
                      serviceName: String = "filter-service"
                    ) {


  override def toString: String = {
    "{ \"id\":\"" + id.toString + "\"," +
      "\"message\":\"" + message + "\"," +
      "\"rejectionName\":\"" + rejectionName + "\"," +
      "\"value\":\"" + value + "\"," +
      "\"rejectionTime\":\"" + rejectionTime + "\"," +
      "\"serviceName\":\"" + serviceName + "\"}"
  }

}

object RejectionDeserializer extends Deserializer[Rejection] {

  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {}

  implicit val formats: Formats = DefaultFormats ++ JavaTypesSerializers.all

  override def close(): Unit = {}

  override def deserialize(_topic: String, data: Array[Byte]): Rejection = {
    read[Rejection](new ByteArrayInputStream(data))
  }
}