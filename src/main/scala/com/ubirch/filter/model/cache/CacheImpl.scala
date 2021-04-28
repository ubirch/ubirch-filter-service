package com.ubirch.filter.model.cache

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.filter.ConfPaths.RedisConfPaths
import com.ubirch.filter.services.Lifecycle
import monix.execution.Scheduler
import org.redisson.Redisson
import org.redisson.api.{ RMapCache, RedissonClient }

import java.net.UnknownHostException
import java.util.concurrent.TimeUnit
import java.util.function.BiConsumer
import javax.inject.{ Inject, Singleton }
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ Future, Promise }

/**
  * Cache implementation for redis with a Map that stores the
  * payload/hash as a key and a boolean as the value that is always true
  */
@Singleton
class CacheImpl @Inject() (lifecycle: Lifecycle, config: Config)(implicit scheduler: Scheduler)
  extends Cache
  with LazyLogging
  with RedisConfPaths {

  private val port: String = config.getString(REDIS_PORT)
  private val password: String = config.getString(REDIS_PASSWORD)
  private val evaluatedPW = if (password == "") null else password
  private val useReplicated: Boolean = config.getBoolean(REDIS_USE_REPLICATED)
  private val filterCacheName: String = config.getString(REDIS_FILTER_CACHE_NAME)
  private val verifyCacheName: String = config.getString(REDIS_VERIFY_CACHE_NAME)
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
  private var verifyCache: RMapCache[Array[Byte], String] = _
  private var filterCache: RMapCache[Array[Byte], String] = _

  private val initialDelay = 1.seconds
  private val repeatingDelay = 2.second

  /**
    * Scheduler trying to connect to redis server with repeating delay if it's not available on startup.
    */
  private val c = scheduler.scheduleAtFixedRate(initialDelay, repeatingDelay) {
    try {
      redisson = Redisson.create(redisConf)
      verifyCache = redisson.getMapCache[Array[Byte], String](verifyCacheName)
      filterCache = redisson.getMapCache[Array[Byte], String](filterCacheName)
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
  def getFromFilterCache(hash: Array[Byte]): Future[Option[String]] = {
    if (filterCache == null)
      Future.failed(NoCacheConnectionException("redis error - a connection could not become established yet"))
    else {
      val p = Promise[Option[String]]()

      val listener = new BiConsumer[String, Throwable] {
        override def accept(t: String, u: Throwable): Unit = {
          if (u != null)
            p.failure(u)
          else
            p.success(Option(t))
        }
      }

      filterCache.getAsync(hash).onComplete(listener)
      p.future
    }
  }

  /**
    * Sets a new key value pair to the cache.
    *
    * @param hash key
    * @return previous associated value for this key or null if
    *         key is set for the first time
    */
  @throws[NoCacheConnectionException]
  def setToFilterCache(hash: Array[Byte], upp: String): Future[Option[String]] = {
    if (filterCache == null)
      Future.failed(NoCacheConnectionException("redis error - a connection could not become established yet"))
    else {
      val p = Promise[Option[String]]()
      val listener = new BiConsumer[String, Throwable] {
        override def accept(t: String, u: Throwable): Unit = {
          if (u != null)
            p.failure(u)
          else
            p.success(Option(t))
        }
      }
      filterCache.putAsync(hash, upp, cacheTTL, TimeUnit.MINUTES).onComplete(listener)
      p.future
    }
  }

  /**
    * Sets a new key value pair to the cache.
    *
    * @param hash key
    * @return previous associated value for this key or null if
    *         key is set for the first time
    */
  @throws[NoCacheConnectionException]
  def setToVerificationCache(hash: Array[Byte], upp: String): Future[Option[String]] = {
    if (verifyCache == null)
      Future.failed(NoCacheConnectionException("redis error - a connection could not become established yet"))
    else {
      val p = Promise[Option[String]]()
      val listener = new BiConsumer[String, Throwable] {
        override def accept(t: String, u: Throwable): Unit = {
          if (u != null)
            p.failure(u)
          else
            p.success(Option(t))
        }
      }
      verifyCache.putAsync(hash, upp, cacheTTL, TimeUnit.MINUTES).onComplete(listener)
      p.future
    }
  }

  /**
    * Sets a new key value pair to the cache.
    *
    * @param hash key
    * @return previous associated value for this key or null if
    *         key is set for the first time
    */
  @throws[NoCacheConnectionException]
  def deleteFromVerificationCache(hash: Array[Byte]): Future[Boolean] = {
    if (verifyCache == null)
      Future.failed(NoCacheConnectionException("redis error - a connection could not become established yet"))
    else {
      Future.successful(verifyCache.fastRemove(hash) == 1)
    }
  }

  lifecycle.addStopHook { () =>
    logger.info("Shutting down Redis: " + filterCacheName + " and " + verifyCacheName)
    Future.successful(redisson.shutdown())
  }

}
