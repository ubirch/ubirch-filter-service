package com.ubirch.filter

import net.manub.embeddedkafka.EmbeddedKafka
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpec}
import os.proc

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

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
