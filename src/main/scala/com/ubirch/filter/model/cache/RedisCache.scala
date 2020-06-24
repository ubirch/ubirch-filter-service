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

import com.typesafe.config.{ Config, ConfigFactory }
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.filter.services.Lifecycle
import javax.inject.{ Inject, Singleton }
import monix.execution.Scheduler
import org.redisson.Redisson
import org.redisson.api.{ RedissonClient, RMap }

import scala.concurrent.duration._
import scala.concurrent.Future

/**
  * Cache implementation for redis with a Map that stores the
  * payload/hash as a key and a boolean as the value that is always true
  */
@Singleton
class RedisCache @Inject() (lifecycle: Lifecycle)(implicit scheduler: Scheduler) extends Cache with LazyLogging {

  def conf: Config = ConfigFactory.load()

  private val port: String = conf.getString("filterService.redis.port")
  private val password: String = conf.getString("filterService.redis.password")
  private val evaluatedPW = if (password == "") null else password
  private val useReplicated: Boolean = conf.getBoolean("filterService.redis.useReplicated")
  private val cacheName: String = conf.getString("filterService.redis.cacheName")
  val redisConf = new org.redisson.config.Config()
  private val prefix = "redis://"

  /**
    * Uses replicated redis server, when used in dev/prod environment.
    */
  if (useReplicated) {
    val mainNode = prefix ++ conf.getString("filterService.redis.mainHost") ++ ":" ++ port
    val replicatedNode = prefix ++ conf.getString("filterService.redis.replicatedHost") ++ ":" ++ port
    redisConf.useReplicatedServers().addNodeAddress(mainNode, replicatedNode).setPassword(evaluatedPW)
  } else {
    val singleNode: String = prefix ++ conf.getString("filterService.redis.host") ++ ":" ++ port
    redisConf.useSingleServer().setAddress(singleNode).setPassword(evaluatedPW)
  }

  private var redisson: RedissonClient = _
  private var cache: RMap[String, Boolean] = _

  private val initialDelay = 1.seconds
  private val repeatingDelay = 2.seconds

  /**
    * Scheduler trying to connect to redis server with repeating delay if it's not available on startup.
    */
  private val c = scheduler.scheduleAtFixedRate(initialDelay, repeatingDelay) {
    try {
      redisson = Redisson.create(redisConf)
      cache = redisson.getMap[String, Boolean](cacheName)
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
  def get(hash: String): Boolean = {
    if (cache == null) throw NoCacheConnectionException("redis error - a connection could not become established yet")
    val result: Any = cache.get(hash)
    //Todo: weird, that in case of absence null is returned
    if (result == null) false else true
  }

  /**
    * Sets a new key value pair to the cache.
    *
    * @param hash key
    * @return previous associated value for this key or null if
    *         key is set for the first time
    */
  @throws[NoCacheConnectionException]
  def set(hash: String): Boolean = {
    if (cache == null) throw NoCacheConnectionException("redis error - a connection could not become established yet")
    cache.put(hash, true)
  }

  lifecycle.addStopHook { () =>
    logger.info("Shutting down Redis: " + cacheName)
    Future.successful(redisson.shutdown())
  }

}
