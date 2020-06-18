package com.ubirch.filter

import java.util.concurrent.CountDownLatch

import com.typesafe.scalalogging.LazyLogging
import com.ubirch.filter.services.kafka.AbstractFilterService
import javax.inject.{Inject, Singleton}

@Singleton
class FilteringSystem @Inject() (filterService: AbstractFilterService) extends LazyLogging {

  def start(): Unit = {
    filterService.start()
    val cd = new CountDownLatch(1)
    cd.await()
  }

}
