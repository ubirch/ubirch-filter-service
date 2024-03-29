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
import com.ubirch.filter.model._
import com.ubirch.filter.model.cache._
import com.ubirch.filter.model.eventlog.Finder
import com.ubirch.filter.testUtils.MessageEnvelopeGenerator.generateMsgEnvelope
import com.ubirch.filter.util.ProtocolMessageUtils.{ base64Encoder, rawPacket }
import com.ubirch.filter.{ Binder, EmbeddedCassandra, InjectorHelper }
import com.ubirch.kafka.util.Exceptions.NeedForPauseException
import com.ubirch.protocol.ProtocolMessage
import com.ubirch.util.cassandra.test.EmbeddedCassandraBase
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{ AsyncWordSpec, BeforeAndAfterAll, MustMatchers }

import java.nio.charset.StandardCharsets
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID
import scala.collection.JavaConverters._
import scala.collection.breakOut
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * This class provides unit tests for most methods of the filter service.
  */
class FilterServiceUnitTests
  extends AsyncWordSpec
  with MockitoSugar
  with MustMatchers
  with LazyLogging
  with EmbeddedCassandraBase
  with BeforeAndAfterAll {

  val cassandra = new CassandraTest

  override protected def beforeAll(): Unit = {
    cassandra.startAndExecuteScripts(EmbeddedCassandra.eventLogCreationCassandraStatements)
  }

  override def afterAll(): Unit = {
    cassandra.stop()
  }

  val cr = new ConsumerRecord[String, String]("topic", 1, 1, "1234", "false")
  private val payload = UUID.randomUUID().toString
  val protocolMessage = new ProtocolMessage(2, UUID.randomUUID(), 0, payload)
  protocolMessage.setSignature("1111".getBytes())
  private val fakeData = ProcessingData(cr, protocolMessage)

  def FakeFilterServiceInjector: InjectorHelper = new InjectorHelper(List(new Binder {
    override def FilterService: ScopedBindingBuilder =
      bind(classOf[AbstractFilterService]).to(classOf[FakeFilterService])
  })) {}

  def InspectVerificationCacheFilterInjector: InjectorHelper = new InjectorHelper(List(new Binder {
    override def Cache: ScopedBindingBuilder = bind(classOf[Cache]).to(classOf[VerificationInspectCache])

    override def FilterService: ScopedBindingBuilder =
      bind(classOf[AbstractFilterService]).to(classOf[FakeFilterService])
  })) {}

  def ExceptionFilterServiceInjector: InjectorHelper = new InjectorHelper(List(new Binder {
    override def FilterService: ScopedBindingBuilder =
      bind(classOf[AbstractFilterService]).to(classOf[ExceptionFilterServ])
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

        override def FilterService: ScopedBindingBuilder =
          bind(classOf[AbstractFilterService]).to(classOf[FakeFilterService])
      })) {}

      val Injector = specialInjector

      val fakeFilter = Injector.get[FakeFilterService]
      val data = ProcessingData(mock[ConsumerRecord[String, String]], protocolMessage)
      fakeFilter.makeVerificationLookup(data).map(_ mustBe None)
    }

    "return Some() if the finder returns not None" in {

      def specialInjector: InjectorHelper = new InjectorHelper(List(new Binder {
        override def Cache: ScopedBindingBuilder = bind(classOf[Cache]).to(classOf[CacheMockAlwaysException])

        override def Finder: ScopedBindingBuilder = bind(classOf[Finder]).to(classOf[CassandraFinderAlwaysFound])

        override def FilterService: ScopedBindingBuilder =
          bind(classOf[AbstractFilterService]).to(classOf[FakeFilterService])
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

        override def FilterService: ScopedBindingBuilder =
          bind(classOf[AbstractFilterService]).to(classOf[FakeFilterService])
      })) {}

      val Injector = specialInjector

      val fakeFilter = Injector.get[FakeFilterService]
      fakeFilter.filterCacheContains(fakeData).map(_ mustBe None)
    }

    "not disturb the forwarding of the UPP in forwardUPP()" in {
      def specialInjector: InjectorHelper = new InjectorHelper(List(new Binder {
        override def Cache: ScopedBindingBuilder = bind(classOf[Cache]).to(classOf[CacheMockAlwaysException])

        override def FilterService: ScopedBindingBuilder =
          bind(classOf[AbstractFilterService]).to(classOf[FakeFilterService])
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

        override def FilterService: ScopedBindingBuilder =
          bind(classOf[AbstractFilterService]).to(classOf[FakeFilterService])
      })) {}

      val Injector = specialInjector

      val fakeFilter = Injector.get[FakeFilterService]
      fakeFilter.filterCacheContains(fakeData).map(_ mustBe Some("lSLEEBePszd8UUFNkp7lAJKTJyEAxAU4OTMxOcRAlwXXt+1SJKEyLPJgr+se58AcNK1y8lO649xvlDQGU5qBmKUXOrZa68OHOhK38kEkEtU50zswfDW2eGokyTQNBQ=="))
    }

    "return false, when hash hasn't been stored to the cache yet" in {
      def specialInjector: InjectorHelper = new InjectorHelper(List(new Binder {
        override def Cache: ScopedBindingBuilder = bind(classOf[Cache]).to(classOf[CacheMockAlwaysFalse])

        override def FilterService: ScopedBindingBuilder =
          bind(classOf[AbstractFilterService]).to(classOf[FakeFilterService])
      })) {}

      val Injector = specialInjector
      val fakeFilter = Injector.get[FakeFilterService]
      fakeFilter.filterCacheContains(fakeData).map(_ mustBe None)
    }

  }

  "cacheIndicatesReplayAttack" must {

    val testCR = new ConsumerRecord[String, String]("test-topic", 3, 3, "test-key", "test-value")
    //create default UPP
    val defaultUPP = generateMsgEnvelope().ubirchPacket
    val defaultAsB64 = base64Encoder.encodeToString(rawPacket(defaultUPP))
    val defaultProcessingData = ProcessingData(testCR, defaultUPP)
    //create disable UPP
    val disableUPP = generateMsgEnvelope(hint = 250).ubirchPacket
    val disableAsB64 = base64Encoder.encodeToString(rawPacket(disableUPP))
    val disableProcessingData = ProcessingData(testCR, disableUPP)
    //create enable UPP
    val enableUPP = generateMsgEnvelope(hint = 251).ubirchPacket
    val enableAsB64 = base64Encoder.encodeToString(rawPacket(enableUPP))
    val enableProcessingData = ProcessingData(testCR, enableUPP)
    //create delete UPP
    val deleteUPP = generateMsgEnvelope(hint = 252).ubirchPacket
    val deleteAsB64 = base64Encoder.encodeToString(rawPacket(deleteUPP))
    val deleteProcessingData = ProcessingData(testCR, deleteUPP)

    "return correct reaction for incoming default UPP" in {

      def specialInjector: InjectorHelper = new InjectorHelper(List(new Binder {
        override def Cache: ScopedBindingBuilder = bind(classOf[Cache]).to(classOf[CacheStoreMock])

        override def FilterService: ScopedBindingBuilder =
          bind(classOf[AbstractFilterService]).to(classOf[FakeFilterService])
      })) {}

      val Injector = specialInjector
      val fakeFilter = Injector.get[FakeFilterService]
      val cache = Injector.get[CacheStoreMock]

      // default UPP returns RejectUPP, when a UPP with that hash has already been stored to the cache
      cache.setMockUpp(Some(defaultAsB64))
      fakeFilter.decideReactionBasedOnCache(defaultProcessingData).map(_ mustBe RejectUPP)

      // default UPP returns InvestigateFurther, when no UPP with that hash has been stored to the cache yet
      cache.setMockUpp(None)
      fakeFilter.decideReactionBasedOnCache(defaultProcessingData).map(_ mustBe InvestigateFurther)
    }

    "return correct reaction for incoming delete UPP" in {

      def specialInjector: InjectorHelper = new InjectorHelper(List(new Binder {
        override def Cache: ScopedBindingBuilder = bind(classOf[Cache]).to(classOf[CacheStoreMock])

        override def FilterService: ScopedBindingBuilder =
          bind(classOf[AbstractFilterService]).to(classOf[FakeFilterService])
      })) {}

      val Injector = specialInjector
      val fakeFilter = Injector.get[FakeFilterService]
      val cache = Injector.get[CacheStoreMock]

      // delete UPP returns RejectUPP, when delete UPP already has been stored to the cache
      cache.setMockUpp(Some(deleteAsB64))
      fakeFilter.decideReactionBasedOnCache(deleteProcessingData).map(_ mustBe RejectUPP)

      // delete UPP returns ForwardUPP, when another UPP than delete has been stored to the cache already
      cache.setMockUpp(Some(defaultAsB64))
      fakeFilter.decideReactionBasedOnCache(deleteProcessingData).map(_ mustBe ForwardUPP)

      // delete UPP returns InvestigateFurther, when no UPP with that hash has been stored to the cache yet
      cache.setMockUpp(None)
      fakeFilter.decideReactionBasedOnCache(deleteProcessingData).map(_ mustBe InvestigateFurther)
    }

    "return correct reaction for incoming enable UPP" in {

      def specialInjector: InjectorHelper = new InjectorHelper(List(new Binder {
        override def Cache: ScopedBindingBuilder = bind(classOf[Cache]).to(classOf[CacheStoreMock])

        override def FilterService: ScopedBindingBuilder =
          bind(classOf[AbstractFilterService]).to(classOf[FakeFilterService])
      })) {}

      val Injector = specialInjector
      val fakeFilter = Injector.get[FakeFilterService]
      val cache = Injector.get[CacheStoreMock]

      // enable UPP returns  RejectUPP, when enable UPP has been stored to the cache already
      cache.setMockUpp(Some(enableAsB64))
      fakeFilter.decideReactionBasedOnCache(enableProcessingData).map(_ mustBe RejectUPP)

      // enable UPP returns RejectUPP, when delete UPP has been stored to the cache already
      cache.setMockUpp(Some(deleteAsB64))
      fakeFilter.decideReactionBasedOnCache(enableProcessingData).map(_ mustBe RejectUPP)

      // enable UPP returns ForwardUPP, when another UPP than enable or delete has been stored to the cache already
      cache.setMockUpp(Some(defaultAsB64))
      fakeFilter.decideReactionBasedOnCache(enableProcessingData).map(_ mustBe ForwardUPP)

      // enable UPP returns ForwardUPP, when another UPP than enable or delete has been stored to the cache already
      cache.setMockUpp(Some(disableAsB64))
      fakeFilter.decideReactionBasedOnCache(enableProcessingData).map(_ mustBe ForwardUPP)

      //enable UPP returns InvestigateFurther, when no UPP with that hash has been stored to the cache yet
      cache.setMockUpp(None)
      fakeFilter.decideReactionBasedOnCache(enableProcessingData).map(_ mustBe InvestigateFurther)
    }

    "return correct reaction for incoming disable UPP" in {

      def specialInjector: InjectorHelper = new InjectorHelper(List(new Binder {
        override def Cache: ScopedBindingBuilder = bind(classOf[Cache]).to(classOf[CacheStoreMock])

        override def FilterService: ScopedBindingBuilder =
          bind(classOf[AbstractFilterService]).to(classOf[FakeFilterService])
      })) {}

      val Injector = specialInjector
      val fakeFilter = Injector.get[FakeFilterService]
      val cache = Injector.get[CacheStoreMock]

      // disable UUP returns RejectUPP, when disable UPP has not been stored to the cache already
      cache.setMockUpp(Some(disableAsB64))
      fakeFilter.decideReactionBasedOnCache(disableProcessingData).map(_ mustBe RejectUPP)

      // disable UUP returns RejectUPP, when delete UPP has not been stored to the cache already
      cache.setMockUpp(Some(deleteAsB64))
      fakeFilter.decideReactionBasedOnCache(disableProcessingData).map(_ mustBe RejectUPP)

      // disable UUP returns ForwardUPP, when another UPP than disable or delete has been stored to the cache already
      cache.setMockUpp(Some(defaultAsB64))
      fakeFilter.decideReactionBasedOnCache(disableProcessingData).map(_ mustBe ForwardUPP)

      // disable UUP returns ForwardUPP, when another UPP than disable or delete has been stored to the cache already
      cache.setMockUpp(Some(enableAsB64))
      fakeFilter.decideReactionBasedOnCache(disableProcessingData).map(_ mustBe ForwardUPP)

      //disable UPP returns InvestigateFurther, when no UPP with that hash has been stored to the cache yet
      cache.setMockUpp(None)
      fakeFilter.decideReactionBasedOnCache(disableProcessingData).map(_ mustBe InvestigateFurther)
    }

  }

  "forwardUPP" must {

    "send the kafka message if the cache works correctly" in {
      val Injector = FakeFilterServiceInjector
      val fakeFilterService = Injector.get[FakeFilterService]
      fakeFilterService.forwardUPP(fakeData)
      assert(fakeFilterService.counter == 1)
    }

    "throw a NeedForPauseException if the send method throws an exception" in {
      val Injector = ExceptionFilterServiceInjector
      val exceptionFilterService = Injector.get[ExceptionFilterServ]
      assertThrows[NeedForPauseException](Await.result(exceptionFilterService.forwardUPP(fakeData), Duration.Inf))
    }

    val cr = new ConsumerRecord[String, String]("topic", 1, 1, "key", "Teststring")
    "insert data to verification cache if forwarded and not of type delete, enable or disable" in {
      val Injector = InspectVerificationCacheFilterInjector
      val filterSvc = Injector.get[FakeFilterService]
      val cache = Injector.get[VerificationInspectCache]
      val pmDefault = generateMsgEnvelope(hint = 0, payload = "768768568afd").ubirchPacket
      val pmDisable = generateMsgEnvelope(hint = 250, payload = "1223478628d").ubirchPacket
      val pmEnable = generateMsgEnvelope(hint = 251, payload = "5356536554").ubirchPacket
      val pmDelete = generateMsgEnvelope(hint = 252, payload = "09090909").ubirchPacket
      val pmDefaultLater = generateMsgEnvelope(hint = 0, payload = "1010101001").ubirchPacket

      filterSvc.deleteFromOrAddToVerificationCache(ProcessingData(cr, pmDefault))
      filterSvc.deleteFromOrAddToVerificationCache(ProcessingData(cr, pmDisable))
      filterSvc.deleteFromOrAddToVerificationCache(ProcessingData(cr, pmEnable))
      filterSvc.deleteFromOrAddToVerificationCache(ProcessingData(cr, pmDelete))
      filterSvc.deleteFromOrAddToVerificationCache(ProcessingData(cr, pmDefaultLater))

      val rDefault = cache.getFromVerificationCache(pmDefault.getPayload.asText().getBytes(StandardCharsets.UTF_8))
      val rDisable = cache.getFromVerificationCache(pmDisable.getPayload.asText().getBytes(StandardCharsets.UTF_8))
      val rEnable = cache.getFromVerificationCache(pmEnable.getPayload.asText().getBytes(StandardCharsets.UTF_8))
      val rDelete = cache.getFromVerificationCache(pmDelete.getPayload.asText().getBytes(StandardCharsets.UTF_8))
      val rDefaultLater =
        cache.getFromVerificationCache(pmDefaultLater.getPayload.asText().getBytes(StandardCharsets.UTF_8))

      val base64Default = base64Encoder.encodeToString(rawPacket(pmDefault))
      val base64DefaultLater = base64Encoder.encodeToString(rawPacket(pmDefaultLater))

      rDefault mustBe Some(base64Default)
      rDefaultLater mustBe Some(base64DefaultLater)
      rDisable mustBe None
      rEnable mustBe None
      rDelete mustBe None
    }

  }

  "reactOnReplayAttack" must {

    "throw a NeedForPauseException if the send method throws an exception" in {
      val message = generateMsgEnvelope()
      val Injector = ExceptionFilterServiceInjector
      val conf = Injector.get[Config]
      val data = ProcessingData(cr, message.ubirchPacket)
      val exceptionFilterService = Injector.get[ExceptionFilterServ]
      assertThrows[NeedForPauseException](Await.result(
        exceptionFilterService.reactOnReplayAttack(data, cr, conf.getString(ProducerConfPaths.REJECTION_TOPIC)),
        Duration.Inf))
    }

    "send the rejectionMessage successfully" in {
      val message = generateMsgEnvelope()
      val Injector = FakeFilterServiceInjector
      val fakeFilterService = Injector.get[FakeFilterService]
      val conf = Injector.get[Config]
      val data = ProcessingData(cr, message.ubirchPacket)
      fakeFilterService.reactOnReplayAttack(data, cr, conf.getString(ProducerConfPaths.REJECTION_TOPIC))
      Thread.sleep(1000)
      assert(fakeFilterService.counter == 1)
    }

    "Add the http headers" in {
      val Injector = FakeFilterServiceInjector
      val fakeFilterService = Injector.get[FakeFilterService]
      val headers: Map[String, String] =
        fakeFilterService.generateReplayAttackProducerRecord(cr, "coucou").headers().asScala.map(h =>
          h.key() -> new String(h.value(), UTF_8))(breakOut)
      headers(Values.HTTP_STATUS_CODE_HEADER) mustBe Values.HTTP_STATUS_CODE_REJECTION_ERROR
    }
  }

}
