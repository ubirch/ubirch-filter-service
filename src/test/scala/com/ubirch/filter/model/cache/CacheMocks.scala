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

import com.ubirch.filter.util.MessageEnvelopeGenerator.{b64, generateMsgEnvelope, rawPacket}

import java.util.UUID
import java.util.concurrent.TimeoutException
import javax.inject.Singleton
import scala.concurrent.Future

/**
  * Different cache mocks for testing purposes.
  */

class CacheMockAlwaysException extends Cache {

  def get(hash: Array[Byte]): Future[Option[String]] = Future.failed(new TimeoutException())

  def set(hash: Array[Byte], upp: String): Future[Unit] = Future.failed(new TimeoutException())
}

class CacheMockAlwaysFalse extends Cache {

  def get(hash: Array[Byte]): Future[Option[String]] = Future.successful(None)

  def set(hash: Array[Byte], upp: String): Future[Unit] = Future.successful(())
}

class CacheMockAlwaysTrue extends Cache {

  private val msgEnv = generateMsgEnvelope(uuid = UUID.fromString("178fb337-7c51-414d-929e-e50092932721"))
  private val b64Env = b64(rawPacket(msgEnv.ubirchPacket))

  def get(hash: Array[Byte]): Future[Option[String]] = Future.successful(Some(b64Env))

  def set(hash: Array[Byte], upp: String): Future[Unit] = Future.successful(())
}

@Singleton
class CacheStoreMock extends Cache {

  private var mockedUPP: Option[String] = None

  def get(hash: Array[Byte]): Future[Option[String]] = Future.successful(mockedUPP)

  def setMockUpp(upp: Option[String]): Unit = {
    mockedUPP = upp
  }

  def set(hash: Array[Byte], upp: String): Future[Unit] = Future.successful(())
}


/**
  * just a cache variable that records what messages are being processed by the filter service
  */
@Singleton
class CustomCache extends Cache {
  var list: List[Array[Byte]] = List[Array[Byte]]()

  def get(hash: Array[Byte]): Future[Option[String]] = {
    list = list :+ hash
    Future.successful(None)
  }

  def set(hash: Array[Byte], upp: String): Future[Unit] = Future.successful(())

}

