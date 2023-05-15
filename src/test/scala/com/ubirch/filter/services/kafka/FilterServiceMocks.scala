package com.ubirch.filter.services.kafka

import com.typesafe.config.Config
import com.ubirch.filter.model.cache.Cache
import com.ubirch.filter.model.eventlog.Finder
import com.ubirch.filter.services.Lifecycle
import com.ubirch.kafka.consumer.ConsumerRunner
import com.ubirch.kafka.producer.ProducerRunner
import monix.execution.Scheduler

import javax.inject.{ Inject, Singleton }
import org.apache.kafka.clients.producer.{ ProducerRecord, RecordMetadata }
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.{ ExecutionContext, Future }

/**
  * A fake filter service that always throws an exception, when the send() method is called.
  *
  * @param cache The cache is only used to record the messages being processed in this test.
  */
@Singleton
class ExceptionFilterServ @Inject() (cache: Cache, finder: Finder, config: Config, lifecycle: Lifecycle)(implicit
val ec: ExecutionContext)
  extends AbstractFilterService(cache, finder, config, lifecycle) {
  implicit val scheduler: Scheduler = Scheduler(ec)

  override def send(producerRecord: ProducerRecord[String, String]): Future[RecordMetadata] = {
    Future.failed {
      new Exception("test exception")
    }
  }
}

/**
  * A fake filter service using mocked Kafka consumer, producer and cache. The send method counts it's calls.
  *
  * @param cache The cache used to check if a message has already been received before.
  */
@Singleton
class FakeFilterService @Inject() (cache: Cache, finder: Finder, config: Config, lifecycle: Lifecycle)(
  implicit override val ec: ExecutionContext,
  override val scheduler: Scheduler)
  extends DefaultFilterService(cache, finder, config, lifecycle)
  with MockitoSugar {
  override lazy val consumption: ConsumerRunner[String, String] = mock[ConsumerRunner[String, String]]
  override lazy val production: ProducerRunner[String, String] = mock[ProducerRunner[String, String]]

  var counter = 0

  override def send(producerRecord: ProducerRecord[String, String]): Future[RecordMetadata] = {
    counter = 1
    Future.successful(mock[RecordMetadata])
  }

  override def send(topic: String, value: String): Future[RecordMetadata] = {
    counter = 1
    Future.successful(mock[RecordMetadata])
  }

}
