package com.ubirch.filter

import com.github.nosan.embedded.cassandra.cql.CqlScript
import com.github.nosan.embedded.cassandra.local.{ LocalCassandraFactory, LocalCassandraFactoryBuilder }
import com.github.nosan.embedded.cassandra.test.TestCassandra
import com.typesafe.scalalogging.LazyLogging

trait EmbeddedCassandra extends LazyLogging {

  val factory: LocalCassandraFactory = new LocalCassandraFactoryBuilder().build()

  val cassandra: TestCassandra = new TestCassandra(factory)

  //startCassandra()

  def startCassandra(): Unit = {
    logger.info("TRYING STARTING CASSI")
    try {
      cassandra.start()
    } catch {
      case e: com.github.nosan.embedded.cassandra.CassandraException =>
        logger.error("CASSI THREW EXCEPTION WHILE STARTING: ", e.getMessage)
        throw e
      case e: Throwable => logger.error("ERROR WHILE STARTING CASSANDRA: ", e.getMessage)
    }
  }
  def stopCassandra(): Unit = {
    cassandra.stop()
  }

  val eventLogCreationCassandraStatement: CqlScript = CqlScript.statements(
    "CREATE KEYSPACE IF NOT EXISTS event_log WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };",
    "USE event_log;",
    "drop MATERIALIZED VIEW IF exists events_by_cat",
    "drop table if exists events;",
    """
      |create table if not exists events (
      |    id text,
      |    customer_id text,
      |    service_class text,
      |    category text,
      |    signature text,
      |    event text,
      |    year int ,
      |    month int,
      |    day int,
      |    hour int,
      |    minute int,
      |    second int,
      |    milli int,
      |    event_time timestamp,
      |    nonce text,
      |    status text,
      |    PRIMARY KEY ((id, category), year, month, day, hour, minute, second, milli)
      |) WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC, hour DESC, minute DESC, second DESC, milli DESC);
    """.stripMargin,
    "drop table if exists lookups;",
    """
      |create table if not exists lookups (
      |    key text,
      |    value text,
      |    name text,
      |    category text,
      |    PRIMARY KEY ((value, category), name)
      |);
    """.stripMargin
  )

}

