package com.ubirch.filter.util

import java.util.concurrent.Executors

import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.ExecutionContext

object ExecutionContextHelper {

  def conf: Config = ConfigFactory.load()

  final val threads = conf.getInt("filterService.threads")

  final val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(threads))


}
