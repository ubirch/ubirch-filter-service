package com.ubirch.filter.model

import com.ubirch.filter.model.eventlog.Finder
import javax.inject.Inject

import scala.concurrent.{ ExecutionContext, Future }

/**
  * Whatever the UPP being queried, the result will always be positive
  */
class CassandraFinderAlwaysFound @Inject() (implicit val ec: ExecutionContext) extends Finder {

  override def findUPP(value: String): Future[Option[String]] = Future.successful(Some(value))

}

/**
  * Whatever the UPP being queried, the result will always be negative
  */
class CassandraFinderNeverFound @Inject() (implicit val ec: ExecutionContext) extends Finder {

  override def findUPP(value: String): Future[Option[String]] = Future.successful(None)

}
