package com.ubirch.filter.models

import com.ubirch.filter.model.eventlog.{CassandraFinder, EventLogRow, EventsDAO}
import javax.inject.Inject

import scala.concurrent.{ExecutionContext, Future}

class CassandraFinderAlwaysFound @Inject() (eventsDAO: EventsDAO)(implicit ec: ExecutionContext) extends CassandraFinder(eventsDAO) {

  override def findUPP(value: String): Future[Option[EventLogRow]] = Future.successful(Some(EventLogRow(value, "", "", "UPP", null, null, null, "", "")))

}

class CassandraFinderNeverFound @Inject() (eventsDAO: EventsDAO)(implicit ec: ExecutionContext) extends CassandraFinder(eventsDAO) {

  override def findUPP(value: String): Future[Option[EventLogRow]] = Future.successful(None)

}
