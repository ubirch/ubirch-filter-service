package com.ubirch.filter.model.eventlog

import com.ubirch.filter.services.cluster.ConnectionService
import io.getquill.{ CassandraAsyncContext, SnakeCase }

import javax.inject._
import scala.concurrent.{ ExecutionContext, Future }

/**
  * Represents the queries linked to the EventLogRow case class and to the Events Table
  *
  * @important
  * Since at least quill 3.12, dynamic query might leads to OutOfMemory.
  * Therefore, we need to avoid using it.
  * @see [[https://github.com/zio/zio-quill/issues/2484]]
  */
trait EventLogQueries extends CassandraBase {

  import db._

  //These represent query descriptions only

  def byIdAndCatQ(id: String, category: String) = quote {
    querySchema[EventLogRow]("events").filter(x => x.id == lift(id) && x.category == lift(category)).map(x => x.id)
  }

}

/**
  * Represent the materialization of the queries. Queries here are actually executed and
  * a concrete connection context is injected.
  * @param connectionService Represents the db connection value that is injected.
  * @param ec Represent the execution context for asynchronous processing.
  */
@Singleton
class EventsDAO @Inject() (val connectionService: ConnectionService)(implicit val ec: ExecutionContext)
  extends EventLogQueries {

  val db: CassandraAsyncContext[SnakeCase] = connectionService.context

  import db._

  //These actually run the queries.
  def byIdAndCat(id: String, category: String): Future[List[String]] = run(byIdAndCatQ(id, category))

}
