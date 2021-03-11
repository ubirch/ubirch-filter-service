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
import com.ubirch.filter.ConfPaths.ProducerConfPaths
import com.ubirch.filter.model.cache.{Cache, CacheMockAlwaysException, CacheMockAlwaysFalse, CacheMockAlwaysTrue}
import com.ubirch.filter.model.eventlog.Finder
import com.ubirch.filter.model.{CassandraFinderAlwaysFound, Values}
import com.ubirch.filter.{Binder, EmbeddedCassandra, InjectorHelper}
import com.ubirch.kafka.MessageEnvelope
import com.ubirch.kafka.util.Exceptions.NeedForPauseException
import com.ubirch.protocol.ProtocolMessage
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.json4s.JObject
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{AsyncWordSpec, BeforeAndAfterAll, MustMatchers}

import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID
import scala.collection.JavaConverters._
import scala.collection.breakOut
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * This class provides unit tests for most methods of the filter service.
  */
class FilterServiceUnitTests extends AsyncWordSpec with MockitoSugar with MustMatchers with LazyLogging with EmbeddedCassandra with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    startCassandra()
    cassandra.executeScripts(eventLogCreationCassandraStatement)
  }

  override def afterAll(): Unit = {
    stopCassandra()
  }

  val cr = new ConsumerRecord[String, String]("topic", 1, 1, "1234", "false")
  private val payload = UUID.randomUUID().toString
  val protocolMessage = new ProtocolMessage(2, UUID.randomUUID(), 0, payload)
  protocolMessage.setSignature("1111".getBytes())
  private val fakeData = ProcessingData(cr, protocolMessage)

  def FakeFilterServiceInjector: InjectorHelper = new InjectorHelper(List(new Binder {
    override def FilterService: ScopedBindingBuilder = bind(classOf[AbstractFilterService]).to(classOf[FakeFilterService])
  })) {}

  def ExceptionFilterServiceInjector: InjectorHelper = new InjectorHelper(List(new Binder {
    override def FilterService: ScopedBindingBuilder = bind(classOf[AbstractFilterService]).to(classOf[ExceptionFilterServ])
  })) {}

  "The extractData() method" must {

    "return None when the ConsumerRecord has a wrong format" in {
      val cr = new ConsumerRecord[String, String]("topic", 1, 1, "1234", "Teststring")

      val Injector = FakeFilterServiceInjector
      val fakeFilterService = Injector.get[FakeFilterService]
      fakeFilterService.extractData(cr) mustBe None
      assert(fakeFilterService.counter == 1)
    }

    "return None when the ConsumerRecord has at least a correct value member" in {
      val cr = new ConsumerRecord[String, String]("topic", 1, 1, "1234", "false")
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
      val data = ProcessingData(mock[ConsumerRecord[String, String]], protocolMessage)
      fakeFilter.makeVerificationLookup(data).map(_ mustBe None)

      //assert(result.code == StatusCodes.NotFound)
    }

    "return Some() if the finder returns not None" in {

      def specialInjector: InjectorHelper = new InjectorHelper(List(new Binder {
        override def Cache: ScopedBindingBuilder = bind(classOf[Cache]).to(classOf[CacheMockAlwaysException])
        override def Finder: ScopedBindingBuilder = bind(classOf[Finder]).to(classOf[CassandraFinderAlwaysFound])
        override def FilterService: ScopedBindingBuilder = bind(classOf[AbstractFilterService]).to(classOf[FakeFilterService])
      })) {}
      val Injector = specialInjector

      val fakeFilter = Injector.get[FakeFilterService]
      val data = ProcessingData(mock[ConsumerRecord[String, String]], protocolMessage)
      fakeFilter.makeVerificationLookup(data).map(_.isDefined mustBe true)
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
      fakeFilter.cacheContainsHash(fakeData).map(_ mustBe None)
    }

    "not disturb the forwarding of the UPP in forwardUPP()" in {
      def specialInjector: InjectorHelper = new InjectorHelper(List(new Binder {
        override def Cache: ScopedBindingBuilder = bind(classOf[Cache]).to(classOf[CacheMockAlwaysException])
        override def FilterService: ScopedBindingBuilder = bind(classOf[AbstractFilterService]).to(classOf[FakeFilterService])
      })) {}
      val Injector = specialInjector

      val fakeFilter = Injector.get[FakeFilterService]
      fakeFilter.forwardUPP(fakeData)
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
      fakeFilter.cacheContainsHash(fakeData).map(_ mustBe Some("value"))
    }

    "return false, when hash hasn't been stored to the cache yet" in {
      def specialInjector: InjectorHelper = new InjectorHelper(List(new Binder {
        override def Cache: ScopedBindingBuilder = bind(classOf[Cache]).to(classOf[CacheMockAlwaysFalse])
        override def FilterService: ScopedBindingBuilder = bind(classOf[AbstractFilterService]).to(classOf[FakeFilterService])
      })) {}
      val Injector = specialInjector
      val fakeFilter = Injector.get[FakeFilterService]
      fakeFilter.cacheContainsHash(fakeData).map(_ mustBe None)
    }

  }

  "forwardUPP" must {

    "send the kafka message if the cache works correctly" in {
      val Injector = FakeFilterServiceInjector
      val fakeFilterService = Injector.get[FakeFilterService]
      fakeFilterService.forwardUPP(fakeData)
      assert(fakeFilterService.counter == 1)
    }

    "throw an NeedForPauseException if the send method throws an exception" in {
      val Injector = ExceptionFilterServiceInjector
      val exceptionFilterService = Injector.get[ExceptionFilterServ]
      assertThrows[NeedForPauseException](Await.result(exceptionFilterService.forwardUPP(fakeData), Duration.Inf))
    }
  }

  "reactOnReplayAttack" must {

    "throw a NeedForPauseException if the send methdos throws an exception" in {
      val message = new MessageEnvelope(new ProtocolMessage(), mock[JObject])
      val Injector = ExceptionFilterServiceInjector
      val conf = Injector.get[Config]
      val exceptionFilterService = Injector.get[ExceptionFilterServ]
      assertThrows[NeedForPauseException](Await.result(exceptionFilterService.reactOnReplayAttack(cr, conf.getString(ProducerConfPaths.REJECTION_TOPIC)), Duration.Inf))
    }

    "send the rejectionMessage successfully" in {
      val Injector = FakeFilterServiceInjector
      val fakeFilterService = Injector.get[FakeFilterService]
      val conf = Injector.get[Config]
      fakeFilterService.reactOnReplayAttack(cr, conf.getString(ProducerConfPaths.REJECTION_TOPIC))
      assert(fakeFilterService.counter == 1)
    }

    "Add the http headers" in {
      val Injector = FakeFilterServiceInjector
      val fakeFilterService = Injector.get[FakeFilterService]
      val headers: Map[String, String] = fakeFilterService.generateReplayAttackProducerRecord(cr, "coucou").headers().asScala.map(h => h.key() -> new String(h.value(), UTF_8))(breakOut)
      headers(Values.HTTP_STATUS_CODE_HEADER) mustBe Values.HTTP_STATUS_CODE_REJECTION_ERROR
    }
  }

}

