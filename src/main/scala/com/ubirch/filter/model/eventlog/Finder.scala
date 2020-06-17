package com.ubirch.filter.model.eventlog

import com.ubirch.filter.model.Values
import javax.inject.{Inject, Singleton}

import scala.concurrent.{ExecutionContext, Future}

trait Finder {

  implicit def ec: ExecutionContext

  def findUPP(value: String): Future[Option[EventLogRow]]

}

/**
* Default instance of the cassandra finder. Connects to cassandra through the
  * @param events
  * @param ec
  */
@Singleton
class CassandraFinder @Inject()(events: EventsDAO)(implicit val ec: ExecutionContext) extends Finder {

  def findUPP(value: String): Future[Option[EventLogRow]] = events.byIdAndCat(value, Values.UPP_CATEGORY).map(_.headOption)

}
