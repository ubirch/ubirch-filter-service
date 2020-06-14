package com.ubirch.filter.model.eventlog

import com.ubirch.filter.services.cluster.ConnectionService
import io.getquill.{CassandraAsyncContext, SnakeCase}
import javax.inject._

import scala.concurrent.{ExecutionContext, Future}

trait LookupKeyQueries extends TablePointer[LookupKeyRow] with CustomEncodings[LookupKeyRow] {

  import db._

  //These represent query descriptions only

  implicit val eventSchemaMeta: db.SchemaMeta[LookupKeyRow] = schemaMeta[LookupKeyRow]("lookups")

  def byValueAndCategoryQ(value: String, category: String): db.Quoted[db.EntityQuery[LookupKeyRow]] = quote {
    query[LookupKeyRow]
      .filter(_.value == lift(value))
      .filter(_.category == lift(category))
  }

}

@Singleton
class Lookups @Inject() (val connectionService: ConnectionService)(implicit val ec: ExecutionContext) extends LookupKeyQueries {

  val db: CassandraAsyncContext[SnakeCase.type] = connectionService.context

  import db._

  //These actually run the queries.

  def byValueAndCategory(value: String, category: String): Future[List[LookupKeyRow]] = {
    run(byValueAndCategoryQ(value, category))
  }

}
