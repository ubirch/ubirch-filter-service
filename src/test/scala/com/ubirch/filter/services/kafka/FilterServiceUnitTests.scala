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

package com.ubirch.filter.services.kafka

import com.google.inject.binder.ScopedBindingBuilder
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.filter.{Binder, EmbeddedCassandra, InjectorHelper}
import com.ubirch.filter.cache.{Cache, CacheMockAlwaysException, CacheMockAlwaysFalse, CacheMockAlwaysTrue}
import com.ubirch.filter.model.eventlog.CassandraFinder
import com.ubirch.filter.services.Lifecycle
import com.ubirch.filter.util.Messages
import com.ubirch.kafka.MessageEnvelope
import com.ubirch.kafka.consumer.ConsumerRunner
import com.ubirch.kafka.producer.ProducerRunner
import com.ubirch.kafka.util.Exceptions.NeedForPauseException
import com.ubirch.protocol.ProtocolMessage
import javax.inject.{Inject, Singleton}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.json4s.JObject
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpec}
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

/**
  * This class provides unit tests for most methods of the filter service.
  */
class FilterServiceUnitTests extends WordSpec with MockitoSugar with MustMatchers with LazyLogging with EmbeddedCassandra with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    startCassandra()
    cassandra.executeScripts(eventLogCreationCassandraStatement)
  }

  override def afterAll(): Unit = {
    stopCassandra()
  }


  val cr = new ConsumerRecord[String, Array[Byte]]("topic", 1, 1, "1234", "false".getBytes)
  val data = ProcessingData(cr, "")

  def FakeFilterServiceInjector: InjectorHelper = new InjectorHelper(List(new Binder {
    override def FilterService: ScopedBindingBuilder = bind(classOf[AbstractFilterService]).to(classOf[FakeFilterService])
  })) {}


  def ExceptionFilterServiceInjector: InjectorHelper = new InjectorHelper(List(new Binder {
    override def FilterService: ScopedBindingBuilder = bind(classOf[AbstractFilterService]).to(classOf[ExceptionFilterServ])
  })) {}

  "The extractData() method" must {

    "return None when the ConsumerRecord has a wrong format" in {
      val cr = new ConsumerRecord[String, Array[Byte]]("topic", 1, 1, "1234", "Teststring".getBytes)

      val Injector = FakeFilterServiceInjector
      val fakeFilterService = Injector.get[FakeFilterService]
      fakeFilterService.extractData(cr) mustBe None
      assert(fakeFilterService.counter == 1)
    }

    "return None when the ConsumerRecord has at least a correct value member" in {
      val cr = new ConsumerRecord[String, Array[Byte]]("topic", 1, 1, "1234", "false".getBytes)
      val Injector = FakeFilterServiceInjector
      val fakeFilterService = Injector.get[FakeFilterService]
      fakeFilterService.extractData(cr) mustBe None
      assert(fakeFilterService.counter == 1)
    }
  }

  "The verification lookup" must {

    "return NotFound if the lookup service returns NotFound" in {

      def specialInjector: InjectorHelper = new InjectorHelper(List(new Binder {
        override def Cache: ScopedBindingBuilder = bind(classOf[Cache]).to(classOf[CacheMockAlwaysException])
        override def FilterService: ScopedBindingBuilder = bind(classOf[AbstractFilterService]).to(classOf[FakeFilterService])
      })) {}
      val Injector = specialInjector

      val fakeFilter = Injector.get[FakeFilterService]
      val data = ProcessingData(mock[ConsumerRecord[String, Array[Byte]]], "")
      import scala.concurrent.duration._
      val result = Await.result(fakeFilter.makeVerificationLookup(data), 5.seconds)
      result mustBe None

      //assert(result.code == StatusCodes.NotFound)
    }

    "return StatusCodes.Ok if the lookup service returns StatusCodes.Ok" in {

      def specialInjector: InjectorHelper = new InjectorHelper(List(new Binder {
        override def Cache: ScopedBindingBuilder = bind(classOf[Cache]).to(classOf[CacheMockAlwaysException])
        override def FilterService: ScopedBindingBuilder = bind(classOf[AbstractFilterService]).to(classOf[FakeFilterService])
      })) {}
      val Injector = specialInjector

      val fakeFilter = Injector.get[FakeFilterService]
      val data = ProcessingData(mock[ConsumerRecord[String, Array[Byte]]], "")
      fakeFilter.makeVerificationLookup(data)
      //assert(result.code == StatusCodes.Ok)
    }

  }

  "Cache exception - when thrown - " must {

    "cause a return false in cacheContainsHash()" in {
      def specialInjector: InjectorHelper = new InjectorHelper(List(new Binder {
        override def Cache: ScopedBindingBuilder = bind(classOf[Cache]).to(classOf[CacheMockAlwaysException])
        override def FilterService: ScopedBindingBuilder = bind(classOf[AbstractFilterService]).to(classOf[FakeFilterService])
      })) {}
      val Injector = specialInjector

      val fakeFilter = Injector.get[FakeFilterService]
      fakeFilter.cacheContainsHash(data) mustBe false
    }

    "not disturb the forwarding of the UPP in forwardUPP()" in {
      def specialInjector: InjectorHelper = new InjectorHelper(List(new Binder {
        override def Cache: ScopedBindingBuilder = bind(classOf[Cache]).to(classOf[CacheMockAlwaysException])
        override def FilterService: ScopedBindingBuilder = bind(classOf[AbstractFilterService]).to(classOf[FakeFilterService])
      })) {}
      val Injector = specialInjector

      val fakeFilter = Injector.get[FakeFilterService]
      fakeFilter.forwardUPP(data)
      assert(fakeFilter.counter == 1)
    }
  }

  "CacheContainsHash" must {

    "return true, when hash already has been stored to cache" in {
      def specialInjector: InjectorHelper = new InjectorHelper(List(new Binder {
        override def Cache: ScopedBindingBuilder = bind(classOf[Cache]).to(classOf[CacheMockAlwaysTrue])
        override def FilterService: ScopedBindingBuilder = bind(classOf[AbstractFilterService]).to(classOf[FakeFilterService])
      })) {}
      val Injector = specialInjector

      val fakeFilter = Injector.get[FakeFilterService]
      fakeFilter.cacheContainsHash(data) mustBe true
    }

    "return false, when hash hasn't been stored to the cache yet" in {
      def specialInjector: InjectorHelper = new InjectorHelper(List(new Binder {
        override def Cache: ScopedBindingBuilder = bind(classOf[Cache]).to(classOf[CacheMockAlwaysFalse])
        override def FilterService: ScopedBindingBuilder = bind(classOf[AbstractFilterService]).to(classOf[FakeFilterService])
      })) {}
      val Injector = specialInjector
      val fakeFilter = Injector.get[FakeFilterService]
      fakeFilter.cacheContainsHash(data) mustBe false
    }

  }

  "forwardUPP" must {

    "send the kafka message if the cache works correctly" in {
      val Injector = FakeFilterServiceInjector
      val fakeFilterService = Injector.get[FakeFilterService]
      fakeFilterService.forwardUPP(data)
      assert(fakeFilterService.counter == 1)
    }

    "throw an NeedForPauseException if the send method throws an exception" in {
      val Injector = ExceptionFilterServiceInjector
      val exceptionFilterService = Injector.get[ExceptionFilterServ]
      assertThrows[NeedForPauseException](Await.result(exceptionFilterService.forwardUPP(data), Duration.Inf))
    }
  }

  "reactOnReplayAttack" must {

    "throw a NeedForPauseException if the send methdos throws an exception" in {
      val message = new MessageEnvelope(new ProtocolMessage(), mock[JObject])
      val Injector = ExceptionFilterServiceInjector
      val exceptionFilterService = Injector.get[ExceptionFilterServ]
      assertThrows[NeedForPauseException](Await.result(exceptionFilterService.reactOnReplayAttack(cr, message, Messages.rejectionTopic), Duration.Inf))
    }

    "send the rejectionMessage successfully" in {
      val Injector = FakeFilterServiceInjector
      val fakeFilterService = Injector.get[FakeFilterService]
      fakeFilterService.reactOnReplayAttack(cr, mock[MessageEnvelope], Messages.rejectionTopic)
      assert(fakeFilterService.counter == 1)
    }
  }

}

/**
  * A fake filter service using mocked Kafka consumer, producer and cache. The send method counts it's calls.
  *
  * @param cache The cache used to check if a message has already been received before.
  */
@Singleton
class FakeFilterService @Inject()(cache: Cache, cassandraFinder: CassandraFinder, config: Config, lifecycle: Lifecycle)(override implicit val ec: ExecutionContext) extends DefaultFilterService(cache: Cache, cassandraFinder, config: Config, lifecycle: Lifecycle) with MockitoSugar {
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
@Singleton
class ExceptionFilterService @Inject()(cache: Cache, cassandraFinder: CassandraFinder, config: Config, lifecycle: Lifecycle)(override implicit val ec: ExecutionContext) extends DefaultFilterService(cache: Cache, cassandraFinder, config: Config, lifecycle: Lifecycle) with MockitoSugar {
  override def send(topic: String, value: Array[Byte]): Future[RecordMetadata] = {
    Future {
      throw new Exception("test exception")
    }
  }
}

