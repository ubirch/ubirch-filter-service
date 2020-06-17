package com.ubirch.filter.models

import com.ubirch.filter.model.eventlog.{ CassandraFinder, EventLogRow, EventsDAO }
import javax.inject.Inject

import scala.concurrent.{ExecutionContext, Future}

/**
* Whatever the UPP being queried, the result will always be positive
  */
class CassandraFinderAlwaysFound @Inject()(events: EventsDAO)(implicit ec: ExecutionContext) extends CassandraFinder(events) {

  override def findUPP(value: String): Future[Option[EventLogRow]] = Future.successful(Some(EventLogRow(value, "", "", "UPP", null, null, null, "", "")))

}

/**
  * Whatever the UPP being queried, the result will always be negative
  */
class CassandraFinderNeverFound @Inject()(events: EventsDAO)(implicit ec: ExecutionContext) extends CassandraFinder(events) {

  override def findUPP(value: String): Future[Option[EventLogRow]] = Future.successful(None)

}
