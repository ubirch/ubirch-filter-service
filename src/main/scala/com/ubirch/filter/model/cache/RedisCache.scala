/*
 * Copyright (c) 2019 ubirch GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ubirch.filter.model.cache

import java.net.UnknownHostException
import java.util.concurrent.TimeUnit

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.filter.ConfPaths.RedisConfPaths
import com.ubirch.filter.services.Lifecycle
import javax.inject.{ Inject, Singleton }
import monix.execution.Scheduler
import org.redisson.Redisson
import org.redisson.api.{ RMapCache, RedissonClient }

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Cache implementation for redis with a Map that stores the
  * payload/hash as a key and a boolean as the value that is always true
  */
@Singleton
class RedisCache @Inject() (lifecycle: Lifecycle, config: Config)(implicit scheduler: Scheduler)
  extends Cache with LazyLogging with RedisConfPaths {

  private val port: String = config.getString(REDIS_PORT)
  private val password: String = config.getString(REDIS_PASSWORD)
  private val evaluatedPW = if (password == "") null else password
  private val useReplicated: Boolean = config.getBoolean(REDIS_USE_REPLICATED)
  private val cacheName: String = config.getString(REDIS_CACHE_NAME)
  private val cacheTTL: Long = config.getLong(REDIS_CACHE_TTL)
  val redisConf = new org.redisson.config.Config()
  private val prefix = "redis://"

  /**
    * Uses replicated redis server, when used in dev/prod environment.
    */
  if (useReplicated) {
    val mainNode = prefix ++ config.getString(REDIS_MAIN_HOST) ++ ":" ++ port
    val replicatedNode = prefix ++ config.getString(REDIS_REPLICATED_HOST) ++ ":" ++ port
    redisConf.useReplicatedServers().addNodeAddress(mainNode, replicatedNode).setPassword(evaluatedPW)
  } else {
    val singleNode: String = prefix ++ config.getString(REDIS_HOST) ++ ":" ++ port
    redisConf.useSingleServer().setAddress(singleNode).setPassword(evaluatedPW)
  }

  private var redisson: RedissonClient = _
  private var cache: RMapCache[Array[Byte], String] = _

  private val initialDelay = 1.seconds
  private val repeatingDelay = 2.seconds

  /**
    * Scheduler trying to connect to redis server with repeating delay if it's not available on startup.
    */
  private val c = scheduler.scheduleAtFixedRate(initialDelay, repeatingDelay) {
    try {
      redisson = Redisson.create(redisConf)
      cache = redisson.getMapCache[Array[Byte], String](cacheName)
      stopConnecting()
      logger.info("connection to redis cache has been established.")
    } catch {
      //Todo: should I differentiate? I don't really implement different behaviour till now at least.
      case ex: UnknownHostException =>
        logger.info("redis error: not able to create connection: ", ex.getMessage, ex)
      case ex: org.redisson.client.RedisConnectionException =>
        logger.info("redis error: not able to create connection: ", ex.getMessage, ex)
      case ex: Exception =>
        logger.info("redis error: not able to create connection: ", ex.getMessage, ex)
    }
  }

  private def stopConnecting(): Unit = {
    c.cancel()
  }

  /**
    * Checks if the hash/payload already is stored in the cache.
    *
    * @param hash key
    * @return value to the key, null if key doesn't exist yet
    */
  @throws[NoCacheConnectionException]
  def get(hash: Array[Byte]): Option[String] = {
    if (cache == null) throw NoCacheConnectionException("redis error - a connection could not become established yet")
    Option(cache.get(hash))
  }

  /**
    * Sets a new key value pair to the cache.
    *
    * @param hash key
    * @return previous associated value for this key or null if
    *         key is set for the first time
    */
  @throws[NoCacheConnectionException]
  def set(hash: Array[Byte], upp: String): Unit = {
    if (cache == null) throw NoCacheConnectionException("redis error - a connection could not become established yet")
    cache.put(hash, upp, cacheTTL, TimeUnit.MINUTES)
  }

  lifecycle.addStopHook { () =>
    logger.info("Shutting down Redis: " + cacheName)
    Future.successful(redisson.shutdown())
  }

}
