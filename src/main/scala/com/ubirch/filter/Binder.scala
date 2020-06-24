package com.ubirch.filter

import com.google.inject.{ AbstractModule, Module }
import com.google.inject.binder.ScopedBindingBuilder
import com.typesafe.config.Config
import com.ubirch.filter.model.cache.{ Cache, RedisCache }
import com.ubirch.filter.model.eventlog.{ CassandraFinder, Finder }
import com.ubirch.filter.services.{ DefaultJVMHook, DefaultLifecycle, JVMHook, Lifecycle }
import com.ubirch.filter.services.cluster.{ ClusterService, ConnectionService, DefaultClusterService, DefaultConnectionService }
import com.ubirch.filter.services.config.ConfigProvider
import com.ubirch.filter.services.execution.{ ExecutionProvider, SchedulerProvider }
import com.ubirch.filter.services.kafka.{ AbstractFilterService, DefaultFilterService }
import monix.execution.Scheduler

import scala.concurrent.ExecutionContext

class Binder extends AbstractModule {

  def Config: ScopedBindingBuilder = bind(classOf[Config]).toProvider(classOf[ConfigProvider])
  def ExecutionContext: ScopedBindingBuilder = bind(classOf[ExecutionContext]).toProvider(classOf[ExecutionProvider])
  def Scheduler: ScopedBindingBuilder = bind(classOf[Scheduler]).toProvider(classOf[SchedulerProvider])
  def Cache: ScopedBindingBuilder = bind(classOf[Cache]).to(classOf[RedisCache])
  def Lifecycle: ScopedBindingBuilder = bind(classOf[Lifecycle]).to(classOf[DefaultLifecycle])
  def JVMHook: ScopedBindingBuilder = bind(classOf[JVMHook]).to(classOf[DefaultJVMHook])
  def ClusterService: ScopedBindingBuilder = bind(classOf[ClusterService]).to(classOf[DefaultClusterService])
  def ConnectionService: ScopedBindingBuilder = bind(classOf[ConnectionService]).to(classOf[DefaultConnectionService])
  def FilterService: ScopedBindingBuilder = bind(classOf[AbstractFilterService]).to(classOf[DefaultFilterService])
  def Finder: ScopedBindingBuilder = bind(classOf[Finder]).to(classOf[CassandraFinder])

  def configure(): Unit = {
    Config
    ExecutionContext
    Scheduler
    Cache
    Lifecycle
    JVMHook
    ClusterService
    ConnectionService
    FilterService
    Finder
  }

}

object Binder {
  def modules: List[Module] = List(new Binder)
}

