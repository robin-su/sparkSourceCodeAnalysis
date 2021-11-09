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

import java.nio.ByteBuffer
import java.util.concurrent.{ExecutorService, RejectedExecutionException}

import scala.language.existentials
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.util.{LongAccumulator, ThreadUtils, Utils}

/**
 * TaskResultGetter用于对序列化的Task执行结果进行反序列化，以得到Task执行结果。TaskResultGetter也可远程获取Task执行结果。
 *
 * Runs a thread pool that deserializes and remotely fetches (if necessary) task results.
 */
private[spark] class TaskResultGetter(sparkEnv: SparkEnv, scheduler: TaskSchedulerImpl)
  extends Logging {

  // 获取Task执行结果的线程数。可通过spark.resultGetter.threads属性配置，默认为4。
  private val THREADS = sparkEnv.conf.getInt("spark.resultGetter.threads", 4)

  // 使用Executors的newFixedThreadPool方法创建的ThreadPool Executor，用于提交获取Task执行结果的线程。线程池的大小由THREADS决定。
  protected val getTaskResultExecutor: ExecutorService =
    ThreadUtils.newDaemonFixedThreadPool(THREADS, "task-result-getter")

  // Exposed for testing.
  /**
   * 类型为ThreadLocal[SerializerInstance]，通过使用本地线程缓存，保证在使用SerializerInstance时是线程安全的。
   */
  protected val serializer = new ThreadLocal[SerializerInstance] {
    override def initialValue(): SerializerInstance = {
      sparkEnv.closureSerializer.newInstance()
    }
  }

  protected val taskResultSerializer = new ThreadLocal[SerializerInstance] {
    override def initialValue(): SerializerInstance = {
      sparkEnv.serializer.newInstance()
    }
  }

  /**
   * TaskResultGetter的enqueueSuccessfulTask方法用于处理执行成功的Task的执行结果
   *
   * @param taskSetManager
   * @param tid
   * @param serializedData
   */
  def enqueueSuccessfulTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      serializedData: ByteBuffer): Unit = {
    getTaskResultExecutor.execute(new Runnable {
      override def run(): Unit = Utils.logUncaughtExceptions {
        try {
          // 对Task的执行结果反序列化
          val (result, size) = serializer.get().deserialize[TaskResult[_]](serializedData) match {
            // 如果Task的结果类型为DirectTaskResult，说明Task的执行结果保存在DirectTaskResult中，
            // 此时只需要对DirectTaskResult保存的数据（即DirectTaskResult的valueBytes属性）进行反序列化就可以得到。
            case directResult: DirectTaskResult[_] =>
              if (!taskSetManager.canFetchMoreResults(serializedData.limit())) {
                // kill the task so that it will not become zombie task
                scheduler.handleFailedTask(taskSetManager, tid, TaskState.KILLED, TaskKilled(
                  "Tasks result size has exceeded maxResultSize"))
                return
              }
              // deserialize "value" without holding any lock so that it won't block other threads.
              // We should call it here, so that when it's called again in
              // "TaskSetManager.handleSuccessfulTask", it does not need to deserialize the value.
              directResult.value(taskResultSerializer.get()) // 获取task执行结果
              (directResult, serializedData.limit())

            /**
             * 对Task的执行结果反序列化，如果Task的结果类型为IndirectTaskResult，说明Task的执行结果没有保存在IndirectTaskResult中，
             * 此时需要调用TaskSchedulerImpl的handleTaskGettingResult方法，
             * 向DAGSchedulerEventProcessLoop投递GettingResultEvent事件，然后调用BlockManager的getRemoteBytes方法，
             * 从运行Task的节点上下载Block，最后对下载到的数据反序列化得到Task的执行结果。
             */
            case IndirectTaskResult(blockId, size) =>
              if (!taskSetManager.canFetchMoreResults(size)) {
                // dropped by executor if size is larger than maxResultSize
                sparkEnv.blockManager.master.removeBlock(blockId)
                // kill the task so that it will not become zombie task
                scheduler.handleFailedTask(taskSetManager, tid, TaskState.KILLED, TaskKilled(
                  "Tasks result size has exceeded maxResultSize"))
                return
              }
              logDebug("Fetching indirect task result for TID %s".format(tid))
              scheduler.handleTaskGettingResult(taskSetManager, tid)
              // 下载结果
              val serializedTaskResult = sparkEnv.blockManager.getRemoteBytes(blockId)
              if (!serializedTaskResult.isDefined) {
                /* We won't be able to get the task result if the machine that ran the task failed
                 * between when the task ended and when we tried to fetch the result, or if the
                 * block manager had to flush the result. */
                scheduler.handleFailedTask(
                  taskSetManager, tid, TaskState.FINISHED, TaskResultLost)
                return
              }
              val deserializedResult = serializer.get().deserialize[DirectTaskResult[_]](
                serializedTaskResult.get.toByteBuffer)
              // force deserialization of referenced value
              deserializedResult.value(taskResultSerializer.get())
              sparkEnv.blockManager.master.removeBlock(blockId)
              (deserializedResult, size)
          }

          // Set the task result size in the accumulator updates received from the executors.
          // We need to do this here on the driver because if we did this on the executors then
          // we would have to serialize the result again after updating the size.
          result.accumUpdates = result.accumUpdates.map { a =>
            if (a.name == Some(InternalAccumulator.RESULT_SIZE)) {
              val acc = a.asInstanceOf[LongAccumulator]
              assert(acc.sum == 0L, "task result size should not have been set on the executors")
              acc.setValue(size.toLong)
              acc
            } else {
              a
            }
          }

          scheduler.handleSuccessfulTask(taskSetManager, tid, result)
        } catch {
          case cnf: ClassNotFoundException =>
            val loader = Thread.currentThread.getContextClassLoader
            taskSetManager.abort("ClassNotFound with classloader: " + loader)
          // Matching NonFatal so we don't catch the ControlThrowable from the "return" above.
          case NonFatal(ex) =>
            logError("Exception while getting task result", ex)
            taskSetManager.abort("Exception while getting task result: %s".format(ex))
        }
      }
    })
  }

  /**
   * 处理执行失败的Task的执行结果
   *
   * @param taskSetManager
   * @param tid
   * @param taskState
   * @param serializedData
   */
  def enqueueFailedTask(taskSetManager: TaskSetManager, tid: Long, taskState: TaskState,
    serializedData: ByteBuffer) {
    var reason : TaskFailedReason = UnknownReason
    try {
      getTaskResultExecutor.execute(new Runnable {
        override def run(): Unit = Utils.logUncaughtExceptions {
          val loader = Utils.getContextOrSparkClassLoader
          try {
            if (serializedData != null && serializedData.limit() > 0) {
              reason = serializer.get().deserialize[TaskFailedReason](
                serializedData, loader) // 对执行结果反序列化，得到失败的原因
            }
          } catch {
            case cnd: ClassNotFoundException =>
              // Log an error but keep going here -- the task failed, so not catastrophic
              // if we can't deserialize the reason.
              logError(
                "Could not deserialize TaskEndReason: ClassNotFound with classloader " + loader)
            case ex: Exception => // No-op
          } finally {
            // If there's an error while deserializing the TaskEndReason, this Runnable
            // will die. Still tell the scheduler about the task failure, to avoid a hang
            // where the scheduler thinks the task is still running.
            scheduler.handleFailedTask(taskSetManager, tid, taskState, reason)
          }
        }
      })
    } catch {
      case e: RejectedExecutionException if sparkEnv.isStopped =>
        // ignore it
    }
  }

  def stop() {
    getTaskResultExecutor.shutdownNow()
  }
}
