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
import org.redisson.Redisson

/**
  * Cache implementation with a Map that stores the
  * payload/hash as a key and a boolean (always true)
  */
object RedisCache extends Cache with LazyLogging {

  def conf: Config = ConfigFactory.load()

  private val host: String = conf.getString("filterService.redis.host")
  private val port: String = conf.getString("filterService.redis.port")
  private val password: String = conf.getString("filterService.redis.password")
  private val useCluster: Boolean = conf.getBoolean("filterService.redis.useCluster")
  private val cacheName: String = conf.getString("filterService.redis.cacheName")
  var redisConf = new org.redisson.config.Config()

  //Todo: Should we use this? => use "rediss://" for SSL connection
  //Todo: Is it ok, to always set the password, even if it's ""?
  if (useCluster)
    redisConf.useClusterServers().addNodeAddress(host ++ port).setPassword(password)
  else
    redisConf.useSingleServer().setAddress(host ++ port).setPassword(password)

  private val redisson = Redisson.create(redisConf)
  private val cache = redisson.getMap[String, Boolean](cacheName)

  /**
    * Checks if the hash/payload already is stored in the cache.
    *
    * @param hash key
    * @return value to the key, null if key doesn't exist yet
    */
  def get(hash: String): Boolean = {
    val result: Any = cache.get(hash)
    //Todo: weird, that in case of absence null is returned
    if (result == null) false else true
  }

  /**
    * Sets a new key value pair to the cache.
    *
    * @param hash key
    * @return previous associated value for this key
    */
  def set(hash: String): Boolean = {
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
