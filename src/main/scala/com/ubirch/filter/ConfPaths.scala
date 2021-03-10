package com.ubirch.filter

object ConfPaths {

  trait CassandraClusterConfPaths {
    final val CONTACT_POINTS = "filterService.cassandra.cluster.contactPoints"
    final val CONSISTENCY_LEVEL = "filterService.cassandra.cluster.consistencyLevel"
    final val SERIAL_CONSISTENCY_LEVEL = "filterService.cassandra.cluster.serialConsistencyLevel"
    final val WITH_SSL = "filterService.cassandra.cluster.withSSL"
    final val USERNAME = "filterService.cassandra.cluster.username"
    final val PASSWORD = "filterService.cassandra.cluster.password"
    final val KEYSPACE = "filterService.cassandra.cluster.keyspace"
    final val PREPARED_STATEMENT_CACHE_SIZE = "filterService.cassandra.cluster.preparedStatementCacheSize"
  }

  trait ConsumerConfPaths {
    final val RECONNECT_BACKOFF_MS_CONFIG = "filterService.kafkaApi.kafkaConsumer.reconnectBackoffMsConfig"
    final val RECONNECT_BACKOFF_MAX_MS_CONFIG = "filterService.kafkaApi.kafkaConsumer.reconnectBackoffMaxMsConfig"
    final val METRICS_SUB_NAMESPACE = "filterService.metrics.prometheus.namespace"
    final val CONSUMER_TOPICS = "filterService.kafkaApi.kafkaConsumer.topic"
    final val CONSUMER_BOOTSTRAP_SERVERS = "filterService.kafkaApi.kafkaConsumer.bootstrapServers"
    final val GROUP_ID = "filterService.kafkaApi.kafkaConsumer.groupId"
    final val MAX_POOL_RECORDS = "filterService.kafkaApi.kafkaConsumer.maxPoolRecords"
    final val GRACEFUL_TIMEOUT = "filterService.kafkaApi.kafkaConsumer.gracefulTimeout"
  }

  trait ProducerConfPaths {
    final val LINGER_MS = "filterService.kafkaApi.kafkaProducer.lingerMS"
    final val PRODUCER_BOOTSTRAP_SERVERS = "filterService.kafkaApi.kafkaProducer.bootstrapServers"
    final val ERROR_TOPIC = "filterService.kafkaApi.kafkaProducer.errorTopic"
    final val FORWARD_TOPIC = "filterService.kafkaApi.kafkaProducer.forwardTopic"
    final val REJECTION_TOPIC = "filterService.kafkaApi.kafkaProducer.rejectionTopic"
  }

  trait FilterConfPaths {
    final val FILTER_STATE = "filterService.stateActive"
    final val ENVIRONMENT = "filterService.verification.environment"
  }

  trait RedisConfPaths {
    final val REDIS_PORT = "filterService.redis.port"
    final val REDIS_PASSWORD = "filterService.redis.password"
    final val REDIS_USE_REPLICATED = "filterService.redis.useReplicated"
    final val REDIS_INDEX = "filterService.redis.index"
    final val REDIS_CACHE_TTL = "filterService.redis.ttl"
    final val REDIS_MAIN_HOST = "filterService.redis.mainHost"
    final val REDIS_REPLICATED_HOST = "filterService.redis.replicatedHost"
    final val REDIS_HOST = "filterService.redis.host"
  }

  object ConsumerConfPaths extends ConsumerConfPaths
  object ProducerConfPaths extends ProducerConfPaths
  object FilterConfPaths extends FilterConfPaths
  object RedisConfPaths extends RedisConfPaths

}
