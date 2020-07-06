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

case class NoCacheConnectionException(
    private val message: String = "",
    private val cause: Throwable = None.orNull
) extends Exception(message, cause)

trait Cache {

  @throws[NoCacheConnectionException]
  def get(hash: Array[Byte]): Option[String]

  @throws[NoCacheConnectionException]
  def set(hash: Array[Byte], upp: String): Unit
}

