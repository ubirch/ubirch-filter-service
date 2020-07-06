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

package com.ubirch.filter.model.cache

import java.util.concurrent.TimeoutException

import javax.inject.Singleton

/**
  * Different cache mocks for testing purposes.
  */

class CacheMockAlwaysException extends Cache {

  def get(hash: Array[Byte]): Option[String] = throw new TimeoutException()

  def set(hash: Array[Byte], upp: String): Unit = throw new TimeoutException()
}

class CacheMockAlwaysFalse extends Cache {

  def get(hash: Array[Byte]): Option[String] = None

  def set(hash: Array[Byte], upp: String): Unit = false
}

class CacheMockAlwaysTrue extends Cache {

  def get(hash: Array[Byte]): Option[String] = Some("value")

  def set(hash: Array[Byte], upp: String): Unit = true
}

/**
  * just a cache variable that records what messages are being processed by the filter service
  */
@Singleton
class CustomCache extends Cache {
  var list: List[Array[Byte]] = List[Array[Byte]]()

  def get(hash: Array[Byte]): Option[String] = {
    list = list :+ hash
    None
  }

  def set(hash: Array[Byte], upp: String): Unit = {
    false
  }
}

