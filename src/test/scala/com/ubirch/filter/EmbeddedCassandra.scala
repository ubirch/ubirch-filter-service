package com.ubirch.filter

import com.github.nosan.embedded.cassandra.cql.StringCqlScript

object EmbeddedCassandra {
  val eventLogCreationCassandraStatements: List[StringCqlScript] = List(
    new StringCqlScript(
      "CREATE KEYSPACE IF NOT EXISTS event_log WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"),
    new StringCqlScript("USE event_log;"),
    new StringCqlScript("drop MATERIALIZED VIEW IF exists events_by_cat"),
    new StringCqlScript("drop table if exists events;"),
    new StringCqlScript(
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
    """.stripMargin),
    new StringCqlScript("drop table if exists lookups;"),
    new StringCqlScript("""
                          |create table if not exists lookups (
                          |    key text,
                          |    value text,
                          |    name text,
                          |    category text,
                          |    PRIMARY KEY ((value, category), name)
                          |);
    """.stripMargin)
  )
}
