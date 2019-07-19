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

import java.util.UUID

import com.softwaremill.sttp.testing.SttpBackendStub
import com.softwaremill.sttp.{Id, Response, StatusCodes}
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.filter.cache.{Cache, CacheMockAlwaysException}
import com.ubirch.kafka.consumer.ConsumerRunner
import com.ubirch.kafka.producer.ProducerRunner
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{MustMatchers, WordSpec}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class FilterServiceUnitTests extends WordSpec with MockitoSugar with MustMatchers with LazyLogging {


  class FakeFilterService(cache: Cache) extends FilterService(cache) {
    override lazy val consumption: ConsumerRunner[String, Array[Byte]] = mock[ConsumerRunner[String, Array[Byte]]]
    override lazy val production: ProducerRunner[String, Array[Byte]] = mock[ProducerRunner[String, Array[Byte]]]

    var counter = 0

    override def send(topic: String, value: Array[Byte]): Future[RecordMetadata] = {
      counter = 1
      Future(mock[RecordMetadata])
    }
  }


  "The extractData() method" must {

    "return None when the ConsumerRecord has wrong format" in {
      val cr = new ConsumerRecord[String, Array[Byte]]("topic", 1, 1, "key", "Teststring".getBytes)
      val fakeFilterService = new FakeFilterService(mock[Cache])
      fakeFilterService.extractData(cr) mustBe None
      assert(fakeFilterService.counter == 1)
    }

    "return None when the ConsumerRecord has nearly correct format" in {
      val cr = new ConsumerRecord[String, Array[Byte]]("topic", 1, 1, "key", "false".getBytes)
      val fakeFilterService = new FakeFilterService(mock[Cache])
      fakeFilterService.extractData(cr) mustBe None
      assert(fakeFilterService.counter == 1)
    }
  }

  "The verification lookup" must {

    "return the status code of the http response" in {
      val filterService = new FilterService(new CacheMockAlwaysException())
      implicit val stub: SttpBackendStub[Id, Nothing] = SttpBackendStub.synchronous
        .whenRequestMatches(_ => true)
        .thenRespondNotFound()
      val payload = UUID.randomUUID().toString
      val data = ProcessingData(mock[ConsumerRecord[String, Array[Byte]]], "")
      val result: Id[Response[String]] = filterService.makeVerificationLookup(data)
      assert(result.code == StatusCodes.NotFound)
    }
  }


  "Cache exception - when thrown - " must {

    "cause a return false in cacheContainsHash()" in {
      val cr = new ConsumerRecord[String, Array[Byte]]("topic", 1, 1, "key", "false".getBytes)
      val data = ProcessingData(cr, "")
      val fakeFilterService = new FakeFilterService(new CacheMockAlwaysException())
      fakeFilterService.cacheContainsHash(data) mustBe false
    }

    "not disturb the forwarding of the UPP in forwardUPP()" in {
      val cr = new ConsumerRecord[String, Array[Byte]]("topic", 1, 1, "key", "false".getBytes)
      val data = ProcessingData(cr, "")
      val fakeFilterService = new FakeFilterService(new CacheMockAlwaysException)
      fakeFilterService.forwardUPP(data)
      assert(fakeFilterService.counter == 1)
    }

  }

}

