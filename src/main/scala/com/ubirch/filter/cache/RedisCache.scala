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

package com.ubirch.filter.cache

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import monix.execution.Scheduler.{global => scheduler}
import org.redisson.Redisson
import org.redisson.api.{RMap, RedissonClient}

import scala.concurrent.duration._

/**
  * Cache implementation for redis with a Map that stores the
  * payload/hash as a key and a boolean as the value that is always true
  */
object RedisCache extends Cache with LazyLogging {

  def conf: Config = ConfigFactory.load()

  private val host: String = conf.getString("filterService.redis.host")
  private val port: String = conf.getString("filterService.redis.port")
  private val password: String = conf.getString("filterService.redis.password")
  private val evaluatedPW = if (password == "") null else password
  private val useCluster: Boolean = conf.getBoolean("filterService.redis.useCluster")
  private val cacheName: String = conf.getString("filterService.redis.cacheName")
  var redisConf = new org.redisson.config.Config()
  private val useSSH = if (conf.getBoolean("filterService.redis.ssl")) "rediss://" else "redis://"

  if (useCluster)
    redisConf.useClusterServers().addNodeAddress(useSSH ++ host ++ ":" ++ port).setPassword(evaluatedPW)
  else
    redisConf.useSingleServer().setAddress(useSSH ++ host ++ ":" ++ port).setPassword(evaluatedPW)

  private var redisson: RedissonClient = null
  private var cache: RMap[String, Boolean] = null

  private val initialDelay = 1.seconds
  private val repeatingDelay = 2.seconds

  private val c = scheduler.scheduleAtFixedRate(initialDelay, repeatingDelay) {
    try {
      redisson = Redisson.create(redisConf)
      cache = redisson.getMap[String, Boolean](cacheName)
      stopConnecting()
      logger.info("connection to redis cache has been established.")
    } catch {
      case ex: Exception =>
        logger.info("redis error: not able to create connection: ", ex.getMessage, ex)
     }
  }

  private def stopConnecting(): Unit= {
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

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {

      logger.info("Shutting down Redis: " + cacheName)
      redisson.shutdown()
      Thread.sleep(1000) //Waiting 1 secs
      logger.info("Bye bye, see you later...")
    }
  })
}
