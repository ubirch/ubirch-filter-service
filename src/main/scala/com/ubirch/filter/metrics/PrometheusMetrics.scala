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

package com.ubirch.filter.metrics

import com.typesafe.config.{ Config, ConfigFactory }
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.filter.services.Lifecycle
import com.ubirch.kafka.metrics.PrometheusMetricsHelper
import io.prometheus.client.exporter.HTTPServer
import javax.inject.{ Inject, Singleton }

import scala.concurrent.Future

@Singleton
class PrometheusMetrics @Inject() (lifecycle: Lifecycle) extends LazyLogging {

  def conf: Config = ConfigFactory.load()

  val port: Int = conf.getInt("filterService.metrics.prometheus.port")

  logger.debug("Creating Prometheus Server on Port[{}]", port)

  val server: HTTPServer = PrometheusMetricsHelper.create(port)

  lifecycle.addStopHook { () =>
    logger.info("Shutting down Prometheus")
    Future.successful(server.stop())
  }

}
