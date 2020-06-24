package com.ubirch.filter

import com.typesafe.config.Config
import com.ubirch.filter.ConfPaths.{ ConsumerConfPaths, ProducerConfPaths }
import net.manub.embeddedkafka.EmbeddedKafka
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpec }
import org.scalatest.concurrent.ScalaFutures
import os.proc

import scala.concurrent.{ Await, Future }
import scala.concurrent.duration.Duration

trait TestBase
  extends WordSpec
  with ScalaFutures
  with BeforeAndAfterEach
  with BeforeAndAfterAll
  with MustMatchers
  with EmbeddedKafka {

  def await[T](future: Future[T]): T = await(future, Duration.Inf)

  def await[T](future: Future[T], atMost: Duration): T = Await.result(future, atMost)

  def getPidOfServiceUsingGivenPort(port: Int): Int = TestBase.getPidOfServiceUsingGivenPort(port)

  def readConsumerTopicHead(conf: Config): String = conf.getString(ConsumerConfPaths.TOPICS).split(", ").toSet.head
  def readProducerForwardTopic(conf: Config): String = conf.getString(ProducerConfPaths.FORWARD_TOPIC)
  def readProducerErrorTopic(conf: Config): String = conf.getString(ProducerConfPaths.ERROR_TOPIC)
  def readProducerRejectionTopic(conf: Config): String = conf.getString(ProducerConfPaths.REJECTION_TOPIC)

}

object TestBase {
  def getPidOfServiceUsingGivenPort(port: Int): Int = {
    proc("lsof", "-t", "-i", s":$port", "-s", "TCP:LISTEN").call().chunks.iterator
      .collect {
        case Left(s) => s
        case Right(s) => s
      }
      .map(x => new String(x.array)).map(_.trim.toInt).toList.head
  }
}
