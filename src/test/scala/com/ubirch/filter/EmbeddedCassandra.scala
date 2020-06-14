package com.ubirch.filter

import com.github.nosan.embedded.cassandra.local.{LocalCassandraFactory, LocalCassandraFactoryBuilder}
import com.github.nosan.embedded.cassandra.test.TestCassandra
import com.typesafe.scalalogging.LazyLogging

trait EmbeddedCassandra extends LazyLogging {

  val factory: LocalCassandraFactory = new LocalCassandraFactoryBuilder().build()

  val cassandra = new TestCassandra(factory)

}

