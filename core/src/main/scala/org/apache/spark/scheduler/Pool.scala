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

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

/**
 * A Schedulable entity that represents collection of Pools or TaskSetManagers
 */
private[spark] class Pool(
    val poolName: String,
    val schedulingMode: SchedulingMode, // 调度模式
    initMinShare: Int,// minShare的初始值。
    initWeight: Int) // weight的初始值
  extends Schedulable with Logging {

  /**
   * 类型为ConcurrentLinkedQueue[Schedulable]，用于存储Schedulable。
   * 由于Schedulable只有Pool和TaskSetManager两个实现类，schedulable-Queue是一个可以嵌套的层次结构
   * Spark的调度池中的调度队列与YARN中调度队列的设计非常相似，也采用了层级队列的管理方式。
   */
  val schedulableQueue = new ConcurrentLinkedQueue[Schedulable]
  /**
   * 调度名称与Schedulable的对应关系。
   */
  val schedulableNameToSchedulable = new ConcurrentHashMap[String, Schedulable]
  /**
   * 用于公平调度算法的权重。
   */
  val weight = initWeight
  /**
   * 用于公平调度算法的参考值。
   */
  val minShare = initMinShare

  /**
   * 当前正在运行的任务数量。
   */
  var runningTasks = 0
  /**
   * 进行调度的优先级
   */
  val priority = 0

  /**
   * 调度池或TaskSetManager所属Stage的身份标识
   */
  var stageId = -1
  /**
   * pool的名称
   */
  val name = poolName
  /**
   * 当前pool的父pool
   */
  var parent: Pool = null

  /**
   * 任务集合的调度算法，默认为FIFOSchedulingAlgorithm。
   */
  private val taskSetSchedulingAlgorithm: SchedulingAlgorithm = {
    schedulingMode match {
      case SchedulingMode.FAIR =>
        new FairSchedulingAlgorithm()
      case SchedulingMode.FIFO =>
        new FIFOSchedulingAlgorithm()
      case _ =>
        val msg = s"Unsupported scheduling mode: $schedulingMode. Use FAIR or FIFO instead."
        throw new IllegalArgumentException(msg)
    }
  }

  /**
   * 1）将 Schedulable添加到schedulableQueue
   * 2）将 Schedulable添加到schedulableNameToSchedulable
   * 3）并将Schedulable的父亲设置为当前Pool。
   *
   * @param schedulable
   */
  override def addSchedulable(schedulable: Schedulable) {
    require(schedulable != null)
    schedulableQueue.add(schedulable)
    schedulableNameToSchedulable.put(schedulable.name, schedulable)
    schedulable.parent = this
  }

  /**
   *  将指定的Schedulable从schedulableQueue和schedulableNameToSchedulable中移除。
   *
   * @param schedulable
   */
  override def removeSchedulable(schedulable: Schedulable) {
    schedulableQueue.remove(schedulable)
    schedulableNameToSchedulable.remove(schedulable.name)
  }

  /**
   * 根据指定名称查找Schedulable。
   *
   * @param schedulableName
   * @return
   */
  override def getSchedulableByName(schedulableName: String): Schedulable = {
    /**
     * 从当前Pool的schedulableNameToSchedulable中查找指定名称的Schedulable
     */
    if (schedulableNameToSchedulable.containsKey(schedulableName)) {
      return schedulableNameToSchedulable.get(schedulableName)
    }

    /**
     * 从当前Pool的schedulableQueue中的各个子Schedulable中查找，直到找到指定名称的Schedulable或者返回null。
     */
    for (schedulable <- schedulableQueue.asScala) {
      val sched = schedulable.getSchedulableByName(schedulableName)
      if (sched != null) {
        return sched
      }
    }
    null
  }

  /**
   * 用于当某个Executor丢失后，调用当前Pool的schedulableQueue中的各个Schedulable（可能为子调度池，也可能是TaskSetManager）的executorLost方法
   *
   * @param executorId
   * @param host
   * @param reason
   */
  override def executorLost(executorId: String, host: String, reason: ExecutorLossReason) {
    schedulableQueue.asScala.foreach(_.executorLost(executorId, host, reason))
  }

  /**
   * checkSpeculatableTasks实际通过迭代调用schedulableQueue中的各个子Schedulable的checkSpeculatableTasks方法来实现。
   *
   * @param minTimeToSpeculation
   * @return
   */
  override def checkSpeculatableTasks(minTimeToSpeculation: Int): Boolean = {
    var shouldRevive = false
    for (schedulable <- schedulableQueue.asScala) {
      shouldRevive |= schedulable.checkSpeculatableTasks(minTimeToSpeculation)
    }
    shouldRevive
  }

  /**
   * getSortedTaskSetQueue方法用于对当前Pool中的所有TaskSetManager按照调度算法进行排序，并返回排序后的TaskSetManager。
   *
   * @return
   */
  override def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager] = {
    val sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]
    val sortedSchedulableQueue =
      schedulableQueue.asScala.toSeq.sortWith(taskSetSchedulingAlgorithm.comparator)
    for (schedulable <- sortedSchedulableQueue) {
      sortedTaskSetQueue ++= schedulable.getSortedTaskSetQueue
    }
    sortedTaskSetQueue
  }

  /**
   * increaseRunningTasks方法用于增加当前Pool及其父Pool中记录的当前正在运行的任务数量。
   *
   * @param taskNum
   */
  def increaseRunningTasks(taskNum: Int) {
    runningTasks += taskNum
    if (parent != null) {
      parent.increaseRunningTasks(taskNum)
    }
  }

  /**
   * decreaseRunningTasks方法用于减少当前Pool及其父Pool中记录的当前正在运行的任务数量。
   *
   * @param taskNum
   */
  def decreaseRunningTasks(taskNum: Int) {
    runningTasks -= taskNum
    if (parent != null) {
      parent.decreaseRunningTasks(taskNum)
    }
  }
}
