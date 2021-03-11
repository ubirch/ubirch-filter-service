package com.ubirch.filter.model.cache

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import com.ubirch.filter.ConfPaths.RedisConfPaths._
import com.ubirch.filter.services.Lifecycle
import monix.execution.Scheduler
import scredis.Redis
import scredis.protocol.AuthConfig

import javax.inject.{Inject, Singleton}
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

@Singleton
class CacheImpl @Inject()(config: Config, lifecycle: Lifecycle)
                         (implicit scheduler: Scheduler) extends Cache with StrictLogging {

  private val host = config.getString(REDIS_MAIN_HOST)
  private val port = config.getInt(REDIS_PORT)
  private val password: String = config.getString(REDIS_PASSWORD)
  private val authOpt =
    if (password == "") None
    else Some(AuthConfig(None, password))
  private val index = config.getInt(REDIS_INDEX)

  private val cacheTTL = config.getInt(REDIS_CACHE_TTL)
  private var redis: Redis = _

  private val initialDelay = 1.seconds
  private val repeatingDelay = 2.seconds

  private val c = scheduler.scheduleAtFixedRate(initialDelay, repeatingDelay) {
    try {
      redis = Redis(host, port, authOpt, index)
      stopConnecting()
      logger.info("connection to redis cache has been established.")
    } catch {
      case ex: Exception =>
        logger.error("redis error: not able to create connection: ", ex.getMessage, ex)
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
  def get(hash: Array[Byte]): Future[Option[String]] = {
    if (redis == null) throw NoCacheConnectionException("redis error - a connection could not become established yet")
    redis.get(new String(hash))
  }

  /**
    * Sets a new key value pair to the cache.
    *
    * @param hash key
    * @return previous associated value for this key or null if
    *         key is set for the first time
    */
  @throws[NoCacheConnectionException]
  def set(hash: Array[Byte], upp: String): Future[Unit] = {
    if (redis == null) Future.failed(NoCacheConnectionException("redis error - a connection could not become established yet"))
    else redis.setEX(new String(hash), upp, cacheTTL * 60)
  }

  lifecycle.addStopHook { () =>
    logger.info("Shutting down Redis if filter service.")
    Future.successful(redis.quit())
  }

}
