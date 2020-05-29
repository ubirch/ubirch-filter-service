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

package com.ubirch.filter.kafka

import com.softwaremill.sttp.testing.SttpBackendStub
import com.softwaremill.sttp.{Id, StatusCodes}
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.filter.cache.{Cache, CacheMockAlwaysException, CacheMockAlwaysFalse, CacheMockAlwaysTrue}
import com.ubirch.filter.util.Messages
import com.ubirch.kafka.MessageEnvelope
import com.ubirch.kafka.consumer.ConsumerRunner
import com.ubirch.kafka.producer.ProducerRunner
import com.ubirch.kafka.util.Exceptions.NeedForPauseException
import com.ubirch.protocol.ProtocolMessage
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.json4s.JObject
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{MustMatchers, WordSpec}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * This class provides unit tests for most methods of the filter service.
  */
class FilterServiceUnitTests extends WordSpec with MockitoSugar with MustMatchers with LazyLogging {

  val cr = new ConsumerRecord[String, Array[Byte]]("topic", 1, 1, "1234", "false".getBytes)
  val data = ProcessingData(cr, "")

  /**
    * A fake filter service using mocked Kafka consumer, producer and cache. The send method counts it's calls.
    *
    * @param cache The cache used to check if a message has already been received before.
    */
  class FakeFilterService(cache: Cache = mock[Cache]) extends FilterService(cache) {
    override lazy val consumption: ConsumerRunner[String, Array[Byte]] = mock[ConsumerRunner[String, Array[Byte]]]
    override lazy val production: ProducerRunner[String, Array[Byte]] = mock[ProducerRunner[String, Array[Byte]]]

    var counter = 0

    override def send(topic: String, value: Array[Byte]): Future[RecordMetadata] = {
      counter = 1
      Future(mock[RecordMetadata])
    }
  }

  /**
    * A filter service, that always throws an exception when the send method is called.
    */
  class ExceptionFilterService() extends FakeFilterService() {
    override def send(topic: String, value: Array[Byte]): Future[RecordMetadata] = {
      Future {
        throw new Exception("test exception")
      }
    }
  }

  "The extractData() method" must {

    "return None when the ConsumerRecord has a wrong format" in {
      val cr = new ConsumerRecord[String, Array[Byte]]("topic", 1, 1, "1234", "Teststring".getBytes)
      val fakeFilterService = new FakeFilterService
      fakeFilterService.extractData(cr) mustBe None
      assert(fakeFilterService.counter == 1)
    }

    "return None when the ConsumerRecord has at least a correct value member" in {
      val cr = new ConsumerRecord[String, Array[Byte]]("topic", 1, 1, "1234", "false".getBytes)
      val fakeFilterService = new FakeFilterService
      fakeFilterService.extractData(cr) mustBe None
      assert(fakeFilterService.counter == 1)
    }
  }

  "The verification lookup" must {

    "return NotFound if the lookup service returns NotFound" in {
      val fakeFilter = new FakeFilterService(new CacheMockAlwaysException())
      implicit val stub: SttpBackendStub[Id, Nothing] = SttpBackendStub.synchronous
        .whenRequestMatches(_ => true)
        .thenRespondNotFound()
      val data = ProcessingData(mock[ConsumerRecord[String, Array[Byte]]], "")
      val result = fakeFilter.makeVerificationLookup(data)
      assert(result.code == StatusCodes.NotFound)
    }

    "return StatusCodes.Ok if the lookup service returns StatusCodes.Ok" in {
      val fakeFilter = new FakeFilterService(new CacheMockAlwaysException())
      implicit val stub: SttpBackendStub[Id, Nothing] = SttpBackendStub.synchronous
        .whenRequestMatches(_ => true)
        .thenRespondOk()
      val data = ProcessingData(mock[ConsumerRecord[String, Array[Byte]]], "")
      val result = fakeFilter.makeVerificationLookup(data)
      assert(result.code == StatusCodes.Ok)
    }

  }

  "Cache exception - when thrown - " must {

    "cause a return false in cacheContainsHash()" in {
      val fakeFilterService = new FakeFilterService(new CacheMockAlwaysException())
      fakeFilterService.cacheContainsHash(data) mustBe false
    }

    "not disturb the forwarding of the UPP in forwardUPP()" in {
      val fakeFilterService = new FakeFilterService(new CacheMockAlwaysException)
      fakeFilterService.forwardUPP(data)
      assert(fakeFilterService.counter == 1)
    }
  }

  "CacheContainsHash" must {

    "return true, when hash already has been stored to cache" in {
      val fakeFilterService = new FakeFilterService(new CacheMockAlwaysTrue)
      fakeFilterService.cacheContainsHash(data) mustBe true
    }

    "return false, when hash hasn't been stored to the cache yet" in {
      val fakeFilterService = new FakeFilterService(new CacheMockAlwaysFalse)
      fakeFilterService.cacheContainsHash(data) mustBe false
    }

  }

  "forwardUPP" must {

    "send the kafka message if the cache works correctly" in {
      val fakeFilterService = new FakeFilterService()
      fakeFilterService.forwardUPP(data)
      assert(fakeFilterService.counter == 1)
    }

    "throw an NeedForPauseException if the send method throws an exception" in {
      val exceptionFilterService = new ExceptionFilterService()
      assertThrows[NeedForPauseException](Await.result(exceptionFilterService.forwardUPP(data), Duration.Inf))
    }
  }

  "reactOnReplayAttack" must {

    "throw a NeedForPauseException if the send methdos throws an exception" in {
      val message = new MessageEnvelope(new ProtocolMessage(), mock[JObject])
      val exceptionFilterService = new ExceptionFilterService()
      assertThrows[NeedForPauseException](Await.result(exceptionFilterService.reactOnReplayAttack(cr, message, Messages.rejectionTopic), Duration.Inf))
    }

    "send the rejectionMessage successfully" in {
      val fakeFilterService = new FakeFilterService()
      fakeFilterService.reactOnReplayAttack(cr, mock[MessageEnvelope], Messages.rejectionTopic)
      assert(fakeFilterService.counter == 1)
    }
  }

}

