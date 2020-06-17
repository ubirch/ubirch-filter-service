package com.ubirch.filter.model.eventlog

import com.ubirch.filter.services.cluster.ConnectionService
import io.getquill.{CassandraAsyncContext, SnakeCase}
import javax.inject._

import scala.concurrent.{ExecutionContext, Future}


/**
  * Represents the queries linked to the EventLogRow case class and to the Events Table
  */
trait EventLogQueries extends TablePointer[EventLogRow] with CustomEncodings[EventLogRow] {

  import db._

  //These represent query descriptions only

  implicit val eventSchemaMeta: db.SchemaMeta[EventLogRow] = schemaMeta[EventLogRow]("events")

  def byIdAndCatQ(id: String, category: String): db.Quoted[db.EntityQuery[EventLogRow]] = quote {
    query[EventLogRow].filter(x => x.id == lift(id) && x.category == lift(category)).map(x => x)
  }

}

/**
  * Represent the materialization of the queries. Queries here are actually executed and
  * a concrete connection context is injected.
  * @param connectionService Represents the db connection value that is injected.
  * @param ec Represent the execution context for asynchronous processing.
  */
@Singleton
class EventsDAO @Inject()(val connectionService: ConnectionService)(implicit val ec: ExecutionContext) extends EventLogQueries {

  val db: CassandraAsyncContext[SnakeCase.type] = connectionService.context

  import db._

  //These actually run the queries.
  def byIdAndCat(id: String, category: String): Future[List[EventLogRow]] = run(byIdAndCatQ(id, category))

}
