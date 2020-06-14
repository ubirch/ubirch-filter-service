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

package com.ubirch.filter

import com.google.inject.{Guice, Injector, Module}
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.filter.metrics.PrometheusMetrics

import scala.reflect.ClassTag
import scala.util.Try

/**
  * Helper to manage Guice Injection.
  */
abstract class InjectorHelper(val modules: List[Module]) extends LazyLogging {

  import InjectorHelper._

  private val injector: Injector = {
    try {
      Guice.createInjector(modules: _*)
    } catch {
      case e: Exception =>
        logger.error("Error Creating Injector: {} ", e.getMessage)
        throw InjectorCreationException(e.getMessage)
    }
  }

  def getAsOption[T](implicit ct: ClassTag[T]): Option[T] = Option(get(ct))

  def get[T](implicit ct: ClassTag[T]): T = get(ct.runtimeClass.asInstanceOf[Class[T]])

  def get[T](clazz: Class[T]): T = {
    try {
      injector.getInstance(clazz)
    } catch {
      case e: Exception =>
        logger.error("Error Injecting: {} ", e.getMessage)
        throw InjectionException(e.getMessage)
    }
  }

  def getAsTry[T](implicit ct: ClassTag[T]): Try[T] = Try(get(ct))

}

object InjectorHelper {
  /**
    * Represents an Exception for when injecting a component
    * @param message Represents the error message
    */
  case class InjectionException(message: String) extends Exception(message)

  /**
    * Represents an Exception for when creating an injector a component
    * @param message Represents the error message
    */
  case class InjectorCreationException(message: String) extends Exception(message)
}

/**
  * Util that integrates an elegant way to add shut down hooks to the JVM.
  */
trait WithPrometheusMetrics {

  _: InjectorHelper =>

  get[PrometheusMetrics]

}

/**
  * Represents an assembly for the boot process
  * @param modules
  */
abstract class Boot(modules: List[Module]) extends InjectorHelper(modules) with WithPrometheusMetrics {
  def *[T](block: => T): Unit =
    try { block } catch {
      case e: Exception =>
        logger.error("Exiting after exception found = {}", e.getMessage)
        Thread.sleep(5000)
        sys.exit(1)
    }
}
