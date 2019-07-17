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
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{MustMatchers, WordSpec}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class FilterServiceUnitTests extends WordSpec with MockitoSugar with MustMatchers with LazyLogging {

  "The extractData() method" must {

    "return None when the ConsumerRecord has wrong format" in {
      val cr = new ConsumerRecord[String, Array[Byte]]("topic", 1, 1, "key", "Teststring".getBytes)
      val filterService = new FilterService(new CacheMockAlwaysException())
      filterService.extractData(cr) mustBe None
    }

    //Todo: Why is this not logging any errormessages but returning None??
    "return None when the ConsumerRecord has nearly correct format" in {
      val cr = new ConsumerRecord[String, Array[Byte]]("topic", 1, 1, "key", "false".getBytes)
      val filterService = new FilterService(new CacheMockAlwaysException())
      //      logger.info("Calling extract data.")
      //      Thread.sleep(10000)
      filterService.extractData(cr) mustBe None
      //      Thread.sleep(10000)
      //      logger.info("Called extract data.")
    }
  }

  "The cassandra lookup" must {

    "return the status code of the http response" in {
      val filterService = new FilterService(new CacheMockAlwaysException())
      implicit val stub: SttpBackendStub[Id, Nothing] = SttpBackendStub.synchronous
        .whenRequestMatches(_ => true) //Todo: add proper requirement? e.g. body == "" but how?
        .thenRespondNotFound()
      val payload = UUID.randomUUID().toString
      val result: Id[Response[String]] = filterService.makeCassandraLookup(payload)
      assert(result.code == StatusCodes.NotFound)
    }
  }


  "Cache exception - when thrown - " must {

    "cause a return false in cacheContainsHash()" in {
      val filterService = new FilterService(new CacheMockAlwaysException())
      filterService.cacheContainsHash("") mustBe false
    }

    "not disturb the forwarding of the UPP in forwardUPP()" in {

      val cr = new ConsumerRecord[String, Array[Byte]]("topic", 1, 1, "key", "false".getBytes)

      class FakeFilterService(cache: Cache) extends FilterService(cache) {
        var counter = 0

        override def send(topic: String, value: Array[Byte]): Future[RecordMetadata] = {
          counter = 1
          Future(mock[RecordMetadata])
        }
      }
      val fakeFilterService = new FakeFilterService(new CacheMockAlwaysException)
      fakeFilterService.forwardUPP(cr, "")
      assert(fakeFilterService.counter == 1)
    }

  }

  "KafkaDownTest" must {
    "do what in?" in {

      val cr = new ConsumerRecord[String, Array[Byte]]("topic", 1, 1, "key", "false".getBytes)

      val filterService = new FilterService(new CacheMockAlwaysException())
      filterService.forwardUPP(cr, "") mustBe false
    }
  }


}

