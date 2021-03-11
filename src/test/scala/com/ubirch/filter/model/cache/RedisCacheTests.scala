package com.ubirch.filter.model.cache

import com.github.sebruck.EmbeddedRedis
import com.google.inject.binder.ScopedBindingBuilder
import com.typesafe.config.{Config, ConfigValueFactory}
import com.ubirch.filter.ConfPaths.RedisConfPaths
import com.ubirch.filter.services.config.ConfigProvider
import com.ubirch.filter.{AsyncTestBase, Binder, InjectorHelper}
import org.scalatest.BeforeAndAfter
import redis.embedded.RedisServer

import java.nio.charset.StandardCharsets

class RedisCacheTests extends AsyncTestBase with EmbeddedRedis with BeforeAndAfter {

  var redis: RedisServer = _

  before {
    redis = new RedisServer(6379)
    Thread.sleep(3000)
    redis.start()
  }

  after {
    redis.stop()
  }

  "Redis cache" must {
    "Set expiration cache accordingly" in {
      def customTestConfigProviderTtl(ttl: Int): ConfigProvider = new ConfigProvider {
        override def conf: Config = super.conf.withValue(
          RedisConfPaths.REDIS_CACHE_TTL,
          ConfigValueFactory.fromAnyRef(ttl)
        )
      }

      def testInjector(ttl: Int): InjectorHelper = new InjectorHelper(List(new Binder {
        override def Config: ScopedBindingBuilder = bind(classOf[Config]).toProvider(customTestConfigProviderTtl(ttl))
      })) {}

      val Injector = testInjector(1)
      Thread.sleep(5000)
      val redisCache = Injector.get[Cache]
      Thread.sleep(5000)
      val hash = "coucou".getBytes(StandardCharsets.UTF_8)
      val upp = "SALUT"
      redisCache.set(hash, upp)
      Thread.sleep(500)
      redisCache.get(hash).map(_ mustBe Some(upp))
      Thread.sleep(60000)
      redisCache.get(hash).map(_ mustBe None)
    }

  }

}
