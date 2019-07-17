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

import java.util.{Date, UUID}

/**
  * Represents the error that is eventually published to Kafka.
  *
  * After trying to process/store the events, there's the possibility of getting an error.
  * This type is used to represent the error generated.
  *
  * @param id            represents the id of the incoming event.
  *                      If the id is not possible to read from the event data, it is automatically generated.
  * @param message       represents a friendly error message.
  * @param exceptionName represents the name of the exception. E.g ParsingIntoEventLogException.
  * @param value         represent the event value.
  *                      It can be empty if a EmptyValueException was thrown or if the exception is not known
  *                      It can be a malformed event if a ParsingIntoEventLogException was thrown
  *                      It can be the well-formed event if a StoringIntoEventLogException was thrown
  * @param errorTime     represents the time when the error occurred
  * @param serviceName   represents the name of the service. By default, we use, event-log-service.
  */
case class Error(
                  id: UUID,
                  message: String,
                  exceptionName: String,
                  value: String = "",
                  errorTime: Date = new Date(),
                  serviceName: String = "filter-service"
                ) {

  //Todo: implement
  override def toString: String = {
    ""
    //EventLogJsonSupport.ToJson[this.type](this).toString
  }
}
