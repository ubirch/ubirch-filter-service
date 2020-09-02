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

import org.apache.kafka.common.serialization.Deserializer
import org.json4s.Formats
import org.json4s.jackson.Serialization.{ read, write }

case class Error(
    error: String,
    causes: Seq[String],
    microservice: String = "filter-service",
    requestId: String
) {

  def toJson(implicit formats: Formats): String = {
    write(this)
  }

}

object Error {

  def ErrorDeserializer(implicit formats: Formats): Deserializer[Error] = new Deserializer[Error] {

    override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {}
    override def close(): Unit = {}
    override def deserialize(topic: String, data: Array[Byte]): Error = {
      read[Error](new ByteArrayInputStream(data))
    }

  }

}
