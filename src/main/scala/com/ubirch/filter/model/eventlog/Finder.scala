package com.ubirch.filter.model.eventlog

import com.ubirch.filter.model.Values
import javax.inject.{ Inject, Singleton }

import scala.concurrent.{ ExecutionContext, Future }

trait Finder {

  implicit def ec: ExecutionContext

  /**
    * Will look for a UPP in the event log table thanks to the uppHash provided
    * @param uppHash hash of the UPP that will be looked up
    * @return the hash of the UPP
    */
  def findUPP(uppHash: String): Future[Option[String]]

}

/**
  * Default instance of the cassandra finder. Connects to cassandra through the events
  */
@Singleton
class CassandraFinder @Inject() (events: EventsDAO)(implicit val ec: ExecutionContext) extends Finder {

  def findUPP(value: String): Future[Option[String]] = events.byIdAndCat(value, Values.UPP_CATEGORY).map(_.headOption)

}
