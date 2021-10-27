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

package org.apache.spark.scheduler

import java.io.{FileInputStream, InputStream}
import java.util.{Locale, NoSuchElementException, Properties}

import scala.util.control.NonFatal
import scala.xml.{Node, XML}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.util.Utils

/**
 * An interface to build Schedulable tree
 * buildPools: build the tree nodes(pools)
 * addTaskSetManager: build the leaf nodes(TaskSetManagers)
 */
private[spark] trait SchedulableBuilder {
  def rootPool: Pool // 返回根调度池

  def buildPools(): Unit // 对调度池进行构建

  def addTaskSetManager(manager: Schedulable, properties: Properties): Unit // 向调度池内添加TaskSetManager
}

private[spark] class FIFOSchedulableBuilder(val rootPool: Pool)
  extends SchedulableBuilder with Logging {

  override def buildPools() {
    // nothing
  }

  override def addTaskSetManager(manager: Schedulable, properties: Properties) {
    // 向根调度池中添加TaskSetManager
    rootPool.addSchedulable(manager)
  }
}

private[spark] class FairSchedulableBuilder(val rootPool: Pool, conf: SparkConf)
  extends SchedulableBuilder with Logging {
//  spark.scheduler.allocation.file
  val SCHEDULER_ALLOCATION_FILE_PROPERTY = "spark.scheduler.allocation.file"
//  调度分配策略文件
  // 用户指定的文件系统中的调度分配文件。
  // 此文件可以通过spark.scheduler.allocation.file属性配置，
  // FairSchedulableBuilder将从文件系统中读取此文件提供的公平调度配置。
  val schedulerAllocFile = conf.getOption(SCHEDULER_ALLOCATION_FILE_PROPERTY)
  // DEFAULT_SCHEDULER_FILE：默认的调度文件名。常量DEFAULT_SCHEDULER_FILE的值固定为"fairscheduler.xml",
  // FairSchedulableBuilder将从ClassPath中读取此文件提供的公平调度配置
  val DEFAULT_SCHEDULER_FILE = "fairscheduler.xml"
  // 此属性的值作为放置TaskSetManager的公平调度池的名称。
  val FAIR_SCHEDULER_PROPERTIES = "spark.scheduler.pool"
  // 默认的调度池名。常量DEFAULT_POOL_NAME的值固定为"default"。
  val DEFAULT_POOL_NAME = "default"
  // ：常量MINIMUM_SHARES_PROPERTY的值固定为"minShare"，即XML文件的<Pool>节点的子节点<mindshare>。
  // 节点<mindshare>的值将作为Pool的minShare属性。
  val MINIMUM_SHARES_PROPERTY = "minShare"
  // 常量SCHEDULING_MODE_PROPERTY的值固定为"schedulingMode"，即XML文件的<Pool>节点的子节点<schedulingMode>。
  // 节点<schedulingMode>的值将作为Pool的调度模式（schedulingMode）属性。
  val SCHEDULING_MODE_PROPERTY = "schedulingMode"
  // 权重属性。常量WEIGHT_PROPERTY的值固定为"weight"，即XML文件的<Pool>节点的子节点<weight>。
  // 节点<weight>的值将作为Pool的权重（weight）属性。
  val WEIGHT_PROPERTY = "weight"
  // 常量POOL_NAME_PROPERTY的值固定为"@name"，即XML文件的<Pool>节点的name属性。
  // name属性的值将作为Pool的调度池名（poolName）属性。
  val POOL_NAME_PROPERTY = "@name"
  // 调度池属性。常量POOLS_PROPERTY的值固定为"pool"。
  val POOLS_PROPERTY = "pool"
  // 调度池属性。常量POOLS_PROPERTY的值固定为"pool"。
  val DEFAULT_SCHEDULING_MODE = SchedulingMode.FIFO
  // 公平调度算法中Schedulable的minShare属性的默认值。常量DEFAULT_MINIMUM_SHARE的值固定为0。
  val DEFAULT_MINIMUM_SHARE = 0
  // 默认的权重。常量DEFAULT_WEIGHT的值固定为1。
  val DEFAULT_WEIGHT = 1

  override def buildPools() {
    var fileData: Option[(InputStream, String)] = None
    try {
      fileData = schedulerAllocFile.map { f =>
        // 从文件系统中获取公平调度配置文件输入流
        val fis = new FileInputStream(f)
        logInfo(s"Creating Fair Scheduler pools from $f")
        Some((fis, f))
      }.getOrElse {
        val is = Utils.getSparkClassLoader.getResourceAsStream(DEFAULT_SCHEDULER_FILE)
        if (is != null) {
          logInfo(s"Creating Fair Scheduler pools from default file: $DEFAULT_SCHEDULER_FILE")
          Some((is, DEFAULT_SCHEDULER_FILE))
        } else {
          logWarning("Fair Scheduler configuration file not found so jobs will be scheduled in " +
            s"FIFO order. To use fair scheduling, configure pools in $DEFAULT_SCHEDULER_FILE or " +
            s"set $SCHEDULER_ALLOCATION_FILE_PROPERTY to a file that contains the configuration.")
          None
        }
      }

      // 解析文件输入流并构建调度池
      fileData.foreach { case (is, fileName) => buildFairSchedulerPool(is, fileName) }
    } catch {
      case NonFatal(t) =>
        val defaultMessage = "Error while building the fair scheduler pools"
        val message = fileData.map { case (is, fileName) => s"$defaultMessage from $fileName" }
          .getOrElse(defaultMessage)
        logError(message, t)
        throw t
    } finally {
      fileData.foreach { case (is, fileName) => is.close() }
    }

    // finally create "default" pool
    buildDefaultPool()
  }

  private def buildDefaultPool() {
    if (rootPool.getSchedulableByName(DEFAULT_POOL_NAME) == null) {
      // 创建默认调度池
      val pool = new Pool(DEFAULT_POOL_NAME, DEFAULT_SCHEDULING_MODE,
        DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT)
      // 向跟调度池的调度队列中添加默认的子调度池
      rootPool.addSchedulable(pool)
      logInfo("Created default pool: %s, schedulingMode: %s, minShare: %d, weight: %d".format(
        DEFAULT_POOL_NAME, DEFAULT_SCHEDULING_MODE, DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT))
    }
  }

  private def buildFairSchedulerPool(is: InputStream, fileName: String) {
    // 将文件输入流转换为XML
    val xml = XML.load(is)
    // 读取XML的每一个<Pool>节点
    for (poolNode <- (xml \\ POOLS_PROPERTY)) {
      // 读取<Pool>的name属性作为调度池的名称
      val poolName = (poolNode \ POOL_NAME_PROPERTY).text

      val schedulingMode = getSchedulingModeValue(poolNode, poolName,
        DEFAULT_SCHEDULING_MODE, fileName)
      val minShare = getIntValue(poolNode, poolName, MINIMUM_SHARES_PROPERTY,
        DEFAULT_MINIMUM_SHARE, fileName)
      val weight = getIntValue(poolNode, poolName, WEIGHT_PROPERTY,
        DEFAULT_WEIGHT, fileName)
      // 将创建的子调度池添加到根调度池的调度队列
      rootPool.addSchedulable(new Pool(poolName, schedulingMode, minShare, weight))

      logInfo("Created pool: %s, schedulingMode: %s, minShare: %d, weight: %d".format(
        poolName, schedulingMode, minShare, weight))
    }
  }

  private def getSchedulingModeValue(
      poolNode: Node,
      poolName: String,
      defaultValue: SchedulingMode,
      fileName: String): SchedulingMode = {

    val xmlSchedulingMode =
      (poolNode \ SCHEDULING_MODE_PROPERTY).text.trim.toUpperCase(Locale.ROOT)
    val warningMessage = s"Unsupported schedulingMode: $xmlSchedulingMode found in " +
      s"Fair Scheduler configuration file: $fileName, using " +
      s"the default schedulingMode: $defaultValue for pool: $poolName"
    try {
      if (SchedulingMode.withName(xmlSchedulingMode) != SchedulingMode.NONE) {
        SchedulingMode.withName(xmlSchedulingMode)
      } else {
        logWarning(warningMessage)
        defaultValue
      }
    } catch {
      case e: NoSuchElementException =>
        logWarning(warningMessage)
        defaultValue
    }
  }

  private def getIntValue(
      poolNode: Node,
      poolName: String,
      propertyName: String,
      defaultValue: Int,
      fileName: String): Int = {

    val data = (poolNode \ propertyName).text.trim
    try {
      data.toInt
    } catch {
      case e: NumberFormatException =>
        logWarning(s"Error while loading fair scheduler configuration from $fileName: " +
          s"$propertyName is blank or invalid: $data, using the default $propertyName: " +
          s"$defaultValue for pool: $poolName")
        defaultValue
    }
  }

  override def addTaskSetManager(manager: Schedulable, properties: Properties) {
    val poolName = if (properties != null) {
        properties.getProperty(FAIR_SCHEDULER_PROPERTIES, DEFAULT_POOL_NAME)
      } else {
        DEFAULT_POOL_NAME
      }
    // 以默认调度池作为TaskManger的父调度池
    var parentPool = rootPool.getSchedulableByName(poolName)
    if (parentPool == null) {
      // we will create a new pool that user has configured in app
      // instead of being defined in xml file
      parentPool = new Pool(poolName, DEFAULT_SCHEDULING_MODE,
        DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT)
      rootPool.addSchedulable(parentPool)
      logWarning(s"A job was submitted with scheduler pool $poolName, which has not been " +
        "configured. This can happen when the file that pools are read from isn't set, or " +
        s"when that file doesn't contain $poolName. Created $poolName with default " +
        s"configuration (schedulingMode: $DEFAULT_SCHEDULING_MODE, " +
        s"minShare: $DEFAULT_MINIMUM_SHARE, weight: $DEFAULT_WEIGHT)")
    }
    parentPool.addSchedulable(manager)
    logInfo("Added task set " + manager.name + " tasks to pool " + poolName)
  }
}
