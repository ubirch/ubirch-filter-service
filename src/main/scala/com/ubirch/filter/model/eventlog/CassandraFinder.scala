package com.ubirch.filter.model.eventlog

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.filter.model.Values
import javax.inject._

import scala.concurrent.{ExecutionContext, Future}

@Singleton
class CassandraFinder @Inject() (eventsDAO: EventsDAO)(implicit ec: ExecutionContext) extends LazyLogging {

  def findUPP(value: String): Future[Option[EventLogRow]] = eventsDAO.events.byIdAndCat(value, Values.UPP_CATEGORY).map(_.headOption)

}
