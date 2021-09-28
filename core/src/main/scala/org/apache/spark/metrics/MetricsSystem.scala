/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.metrics

import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.collection.mutable

import com.codahale.metrics.{Metric, MetricFilter, MetricRegistry}
import org.eclipse.jetty.servlet.ServletContextHandler

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.metrics.sink.{MetricsServlet, Sink}
import org.apache.spark.metrics.source.{Source, StaticSources}
import org.apache.spark.util.Utils

/**
 * Spark Metrics System, created by a specific "instance", combined by source,
 * sink, periodically polls source metrics data to sink destinations.
 *
 * "instance" specifies "who" (the role) uses the metrics system. In Spark, there are several roles
 * like master, worker, executor, client driver. These roles will create metrics system
 * for monitoring. So, "instance" represents these roles. Currently in Spark, several instances
 * have already implemented: master, worker, executor, driver, applications.
 *
 * "source" specifies "where" (source) to collect metrics data from. In metrics system, there exists
 * two kinds of source:
 *   1. Spark internal source, like MasterSource, WorkerSource, etc, which will collect
 *   Spark component's internal state, these sources are related to instance and will be
 *   added after a specific metrics system is created.
 *   2. Common source, like JvmSource, which will collect low level state, is configured by
 *   configuration and loaded through reflection.
 *
 * "sink" specifies "where" (destination) to output metrics data to. Several sinks can
 * coexist and metrics can be flushed to all these sinks.
 *
 * Metrics configuration format is like below:
 * [instance].[sink|source].[name].[options] = xxxx
 *
 * [instance] can be "master", "worker", "executor", "driver", "applications" which means only
 * the specified instance has this property.
 * wild card "*" can be used to replace instance name, which means all the instances will have
 * this property.
 *
 * [sink|source] means this property belongs to source or sink. This field can only be
 * source or sink.
 *
 * [name] specify the name of sink or source, if it is custom defined.
 *
 * [options] represent the specific property of this source or sink.
 */
private[spark] class MetricsSystem private (
    val instance: String, // 度量系统的实例名
    conf: SparkConf,
    securityMgr: SecurityManager)
  extends Logging {

  /**
   * 度量配置。metricsConfig的类型为MetricsConfig，主要提供对度量配置的设置、加载、转换等功能。
   * MetricsConfig中的度量配置包括了Sink和Source，因此MetricsSystem将根据MetricsConfig构建度量系统的所有Sink和Source。
   */
  private[this] val metricsConfig = new MetricsConfig(conf)

  /**
   * Sink的数组。Sink（即度量输出）sinks用于缓存所有注册到MetricsSystem的度量输出。
   */
  private val sinks = new mutable.ArrayBuffer[Sink]
  /**
   * Source的数组：即度量的采集或来源，sources用于缓存所有注册到MetricsSystem的Source。
   */
  private val sources = new mutable.ArrayBuffer[Source]
  /**
   * 度量注册点MetricRegistry。Source和Sink实际都是通过MetricRegistry注册到Metrics的度量仓库中的。
   * Metrics是codahale提供的第三方度量仓库，这里的MetricRegistry是Metrics提供的API
   */
  private val registry = new MetricRegistry()

  /**
   * 度量注册点MetricRegistry。Source和Sink实际都是通过MetricRegistry注册到Metrics的度量仓库中的。
   * Metrics是codahale提供的第三方度量仓库，这里的MetricRegistry是Metrics提供的API
   */
  private var running: Boolean = false

  /**
   * metricsServlet将在添加ServletContextHandler后通过WebUI展示。metricsServlet的类型是Option[MetricsServlet]。
   */
  private var metricsServlet: Option[MetricsServlet] = None

  /**
   * Get any UI handlers used by this metrics system; can only be called after start().
   */
  def getServletHandlers: Array[ServletContextHandler] = {
    require(running, "Can only call getServletHandlers on a running MetricsSystem")
    metricsServlet.map(_.getHandlers(conf)).getOrElse(Array())
  }

  metricsConfig.initialize()

  def start(registerStaticSources: Boolean = true) {
    require(!running, "Attempting to start a MetricsSystem that is already running")
    // 将当前MetricsSystem的running字段置为true，即表示MetricsSystem已经处于运行状态。
    running = true
    if (registerStaticSources) {
      StaticSources.allSources.foreach(registerSource)
      registerSources()
    }
    registerSinks()
    sinks.foreach(_.start)
  }

  def stop() {
    if (running) {
      sinks.foreach(_.stop)
    } else {
      logWarning("Stopping a MetricsSystem that is not running")
    }
    running = false
  }

  def report() {
    sinks.foreach(_.report())
  }

  /**
   * 1.构建度量源的注册名
   *
   * Build a name that uniquely identifies each metric source.
   * The name is structured as follows: <app ID>.<executor ID (or "driver")>.<source name>.
   * If either ID is not available, this defaults to just using <source name>.
   *
   * @param source Metric source to be named by this method.
   * @return A unique metric name for each combination of
   *         application, executor/driver and metric source.
   */
  private[spark] def buildRegistryName(source: Source): String = {
//    度量命名空间
    val metricsNamespace = conf.get(METRICS_NAMESPACE).orElse(conf.getOption("spark.app.id"))
//    当前Executor的身份标识
    val executorId = conf.getOption("spark.executor.id")
//    调用MetricRegistry的name方法
    val defaultName = MetricRegistry.name(source.sourceName)

    if (instance == "driver" || instance == "executor") {
      if (metricsNamespace.isDefined && executorId.isDefined) {
        MetricRegistry.name(metricsNamespace.get, executorId.get, source.sourceName)
      } else {
        // Only Driver and Executor set spark.app.id and spark.executor.id.
        // Other instance types, e.g. Master and Worker, are not related to a specific application.
        if (metricsNamespace.isEmpty) {
          logWarning(s"Using default name $defaultName for source because neither " +
            s"${METRICS_NAMESPACE.key} nor spark.app.id is set.")
        }
        if (executorId.isEmpty) {
          logWarning(s"Using default name $defaultName for source because spark.executor.id is " +
            s"not set.")
        }
        defaultName
      }
    } else { defaultName }
  }

  def getSourcesByName(sourceName: String): Seq[Source] =
    sources.filter(_.sourceName == sourceName)

  // 用于向MetricsSystem中注册度量源
  def registerSource(source: Source) {
    sources += source
    try {
      val regName = buildRegistryName(source)
      registry.register(regName, source.metricRegistry)
    } catch {
      case e: IllegalArgumentException => logInfo("Metrics already registered", e)
    }
  }

  def removeSource(source: Source) {
    sources -= source
    val regName = buildRegistryName(source)
    registry.removeMatching(new MetricFilter {
      def matches(name: String, metric: Metric): Boolean = name.startsWith(regName)
    })
  }

  private def registerSources() {
    val instConfig = metricsConfig.getInstance(instance)
    val sourceConfigs = metricsConfig.subProperties(instConfig, MetricsSystem.SOURCE_REGEX)

    // Register all the sources related to instance
    sourceConfigs.foreach { kv =>
      val classPath = kv._2.getProperty("class")
      try {
        val source = Utils.classForName(classPath).newInstance()
        registerSource(source.asInstanceOf[Source])
      } catch {
        case e: Exception => logError("Source class " + classPath + " cannot be instantiated", e)
      }
    }
  }

  private def registerSinks() {
    val instConfig = metricsConfig.getInstance(instance)
    val sinkConfigs = metricsConfig.subProperties(instConfig, MetricsSystem.SINK_REGEX)

    sinkConfigs.foreach { kv =>
      val classPath = kv._2.getProperty("class")
      if (null != classPath) {
        try {
          val sink = Utils.classForName(classPath)
            .getConstructor(classOf[Properties], classOf[MetricRegistry], classOf[SecurityManager])
            .newInstance(kv._2, registry, securityMgr)
          if (kv._1 == "servlet") {
            metricsServlet = Some(sink.asInstanceOf[MetricsServlet])
          } else {
            sinks += sink.asInstanceOf[Sink]
          }
        } catch {
          case e: Exception =>
            logError("Sink class " + classPath + " cannot be instantiated")
            throw e
        }
      }
    }
  }
}

private[spark] object MetricsSystem {
  val SINK_REGEX = "^sink\\.(.+)\\.(.+)".r
  val SOURCE_REGEX = "^source\\.(.+)\\.(.+)".r

  private[this] val MINIMAL_POLL_UNIT = TimeUnit.SECONDS
  private[this] val MINIMAL_POLL_PERIOD = 1

  def checkMinimalPollingPeriod(pollUnit: TimeUnit, pollPeriod: Int) {
    val period = MINIMAL_POLL_UNIT.convert(pollPeriod, pollUnit)
    if (period < MINIMAL_POLL_PERIOD) {
      throw new IllegalArgumentException("Polling period " + pollPeriod + " " + pollUnit +
        " below than minimal polling period ")
    }
  }

  def createMetricsSystem(
      instance: String, conf: SparkConf, securityMgr: SecurityManager): MetricsSystem = {
    new MetricsSystem(instance, conf, securityMgr)
  }
}
