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

import org.redisson.Redisson

/**
  * Cache implementation with a Map that stores the
  * payload/hash as a key and a boolean (always true)
  */
object RedisCache extends Cache {


  //Todo: use conf file and not default values?
  //  protected val config: Config = ConfigFactory.load("application.base.conf")
  // val conf = Try(config.getConfig("redisson"))

  private val redisson = Redisson.create()
  private val cache = redisson.getMap[String, Boolean]("UPP-hashes")

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
}
