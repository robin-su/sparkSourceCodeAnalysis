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

package org.apache.spark

import java.util.concurrent.{ScheduledFuture, TimeUnit}

import scala.collection.mutable
import scala.concurrent.Future

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util._

/**
 * A heartbeat from executors to the driver. This is a shared message used by several internal
 * components to convey liveness or execution information for in-progress tasks. It will also
 * expire the hosts that have not heartbeated for more than spark.network.timeout.
 * spark.executor.heartbeatInterval should be significantly less than spark.network.timeout.
 */
private[spark] case class Heartbeat(
    executorId: String,
    accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])], // taskId -> accumulator updates
    blockManagerId: BlockManagerId)

/**
 * An event that SparkContext uses to notify HeartbeatReceiver that SparkContext.taskScheduler is
 * created.
 */
private[spark] case object TaskSchedulerIsSet

private[spark] case object ExpireDeadHosts

private case class ExecutorRegistered(executorId: String)

private case class ExecutorRemoved(executorId: String)

/**
 * HeartbeatResponse消息携带的reregisterBlockManager表示是否要求Executor重新向BlockManagerMaster注册BlockManager。
 *
 * @param reregisterBlockManager
 */
private[spark] case class HeartbeatResponse(reregisterBlockManager: Boolean)

/**
 * HeartbeatReceiver运行在Driver上，用以接收各个Executor的心跳（HeartBeat）消息，对各个Executor的“生死”进行监控
 *
 * Lives in the driver to receive heartbeats from executors..
 */
private[spark] class HeartbeatReceiver(sc: SparkContext, clock: Clock)
  extends SparkListener with ThreadSafeRpcEndpoint with Logging {

  def this(sc: SparkContext) {
    this(sc, new SystemClock)
  }

  sc.listenerBus.addToManagementQueue(this)

  /**
   * 即SparkEnv的子组件RpcEnv。
   */
  override val rpcEnv: RpcEnv = sc.env.rpcEnv

  /**
   * 即TaskSchedulerImpl。
   */
  private[spark] var scheduler: TaskScheduler = null

  /**
   * 用于维护Executor的身份标识与HeartbeatReceiver最后一次收到Executor的心跳（HeartBeat）消息的时间戳之间的映射关系。
   */
  // executor ID -> timestamp of when the last heartbeat from this executor was received
  private val executorLastSeen = new mutable.HashMap[String, Long]

  // "spark.network.timeout" uses "seconds", while `spark.storage.blockManagerSlaveTimeoutMs` uses
  // "milliseconds"
  /**
   * Executor节点上的BlockManager的超时时间（单位为ms）。可通过spark.storage.blockManagerSlaveTimeoutMs属性配置，
   * 默认为120000。
   */
  private val executorTimeoutMs =
    sc.conf.getTimeAsMs("spark.storage.blockManagerSlaveTimeoutMs",
      s"${sc.conf.getTimeAsSeconds("spark.network.timeout", "120s")}s")

  // "spark.network.timeoutInterval" uses "seconds", while
  // "spark.storage.blockManagerTimeoutIntervalMs" uses "milliseconds"
  /**
   * 超时的间隔（单位为ms）。可通过spark.storage.blockManagerTime-outIntervalMs属性配置，默认为60000。
   */
  private val timeoutIntervalMs = {
    sc.conf.getTimeAsMs("spark.storage.blockManagerTimeoutIntervalMs", "60s")
  }
  /**
   * 检查超时的间隔（单位为ms）。可通过spark.network.time-outInterval属性配置，默认采用timeoutIntervalMs的值。
   */
  private val checkTimeoutIntervalMs =
    sc.conf.getTimeAsSeconds("spark.network.timeoutInterval", s"${timeoutIntervalMs}ms") * 1000

  private var timeoutCheckingTask: ScheduledFuture[_] = null

  // "eventLoopThread" is used to run some pretty fast actions. The actions running in it should not
  // block the thread for a long time.
  /**
   * 类型为ScheduledThreadPoolExecutor，用于执行心跳接收器的超时检查任务，eventLoopThread只包含一个线程，
   * 此线程以heartbeat-receiver-event-loop-thread作为名称。
   */
  private val eventLoopThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("heartbeat-receiver-event-loop-thread")

  /**
   * 以Executors.newSingleThreadExecutor方式创建的ExecutorService，运行的单线程用于“杀死”（kill）Executor，此线程以kill-executor-thread作为名称。
   * 一个单守护线程的普通线程池，其名称为kill-executor-thread，用来异步执行杀掉Executor的任务。
   */
  private val killExecutorThread = ThreadUtils.newDaemonSingleThreadExecutor("kill-executor-thread")

  /**
   * HeartbeatReceiver作为一个RPC端点，实现了RpcEndpoint.onStart()方法，当RPC环境中的Dispatcher注册RPC端点时，会调用该方法。
   */
  override def onStart(): Unit = {
    /**
     * 在HeartbeatReceiver启动时，会让eventLoopThread开始以spark.network.timeoutInterval规定的间隔调度执行，
     * 并将ScheduledFuture对象返回给timeoutCheckingTask。该线程只做一件事，就是向HeartbeatReceiver自己发送
     * ExpireDeadHosts消息，并等待回复
     */
    timeoutCheckingTask = eventLoopThread.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        Option(self).foreach(_.ask[Boolean](ExpireDeadHosts))
      }
    }, 0, checkTimeoutIntervalMs, TimeUnit.MILLISECONDS)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

    // Messages sent and received locally
    /**
     * 将Executor ID与通过SystemClock获取的当前时间戳加入executorLastSeen映射中，并回复true。
     */
    case ExecutorRegistered(executorId) =>
      executorLastSeen(executorId) = clock.getTimeMillis()
      context.reply(true)
    /**
     * 从executorLastSeen映射中删除Executor ID对应的条目，并回复true
     */
    case ExecutorRemoved(executorId) =>
      executorLastSeen.remove(executorId)
      context.reply(true)

    /**
     * 该消息的含义是TaskScheduler已经生成并准备好，在SparkContext初始化过程中会发送此消息.
     * 收到该消息后会令HeartbeatReceiver也持有一份TaskScheduler实例，并回复true。
     */
    case TaskSchedulerIsSet =>
      scheduler = sc.taskScheduler
      context.reply(true)
    /**
     * 该消息的含义是清理那些由于太久没发送心跳而超时的Executor，会调用expireDeadHosts()方法并回复true
     */
    case ExpireDeadHosts =>
      expireDeadHosts()
      context.reply(true)

    // Messages received from executors
    case heartbeat @ Heartbeat(executorId, accumUpdates, blockManagerId) =>
      // 若TaskScheduler不为空
      if (scheduler != null) {
        // executorLastSeen映射中已经保存有ExecutorID
        if (executorLastSeen.contains(executorId)) {
          // 就更新时间戳，
          executorLastSeen(executorId) = clock.getTimeMillis()
          // 并向eventLoopThread线程提交执行TaskScheduler.executorHeartbeatReceived()方法
          // （该方法用于通知Master，使其知道BlockManager是存活状态），并回复HeartbeatResponse消
          eventLoopThread.submit(new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
              val unknownExecutor = !scheduler.executorHeartbeatReceived(
                executorId, accumUpdates, blockManagerId)
              val response = HeartbeatResponse(reregisterBlockManager = unknownExecutor)
              context.reply(response)
            }
          })
        } else {
          /**
           * 在HeartbeatResponse消息中注明需要重新注册BlockManager。
           */
          // This may happen if we get an executor's in-flight heartbeat immediately
          // after we just removed it. It's not really an error condition so we should
          // not log warning here. Otherwise there may be a lot of noise especially if
          // we explicitly remove executors (SPARK-4134).
          logDebug(s"Received heartbeat from unknown executor $executorId")
          context.reply(HeartbeatResponse(reregisterBlockManager = true))
        }
      } else {
        // Because Executor will sleep several seconds before sending the first "Heartbeat", this
        // case rarely happens. However, if it really happens, log it and ask the executor to
        // register itself again.
        /**
         * 至于executorLastSeen映射中不包含当前Executor ID，或者TaskScheduler为空的情况，都会直接回复需要重新注册
         * BlockManager的HeartbeatResponse消息。
         */
        logWarning(s"Dropping $heartbeat because TaskScheduler is not ready yet")
        context.reply(HeartbeatResponse(reregisterBlockManager = true))
      }
  }

  /**
   * Send ExecutorRegistered to the event loop to add a new executor. Only for test.
   *
   * @return if HeartbeatReceiver is stopped, return None. Otherwise, return a Some(Future) that
   *         indicate if this operation is successful.
   */
  def addExecutor(executorId: String): Option[Future[Boolean]] = {
    Option(self).map(_.ask[Boolean](ExecutorRegistered(executorId)))
  }

  /**
   * 我们知道事件总线在接收到SparkListenerExecutorAdded消息后，将调用HeartbeatReceiver的onExecutorAdded方法，
   * 这样HeartbeatReceiver将监听到Executor的添加。
   *
   * If the heartbeat receiver is not stopped, notify it of executor registrations.
   */
  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    addExecutor(executorAdded.executorId)
  }

  /**
   * HeartbeatReceiver继承了SparkListener，并实现了onExecutorRemoved方法，这样HeartbeatReceiver将监听到Executor的移除。
   *
   * Send ExecutorRemoved to the event loop to remove an executor. Only for test.
   *
   * @return if HeartbeatReceiver is stopped, return None. Otherwise, return a Some(Future) that
   *         indicate if this operation is successful.
   */
  def removeExecutor(executorId: String): Option[Future[Boolean]] = {
    Option(self).map(_.ask[Boolean](ExecutorRemoved(executorId)))
  }

  /**
   * If the heartbeat receiver is not stopped, notify it of executor removals so it doesn't
   * log superfluous errors.
   *
   * Note that we must do this after the executor is actually removed to guard against the
   * following race condition: if we remove an executor's metadata from our data structure
   * prematurely, we may get an in-flight heartbeat from the executor before the executor is
   * actually removed, in which case we will still mark the executor as a dead host later
   * and expire it with loud error messages.
   */
  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    removeExecutor(executorRemoved.executorId)
  }

  /**
   * 清理超时的Executor
   */
  private def expireDeadHosts(): Unit = {
    logTrace("Checking for hosts with no recent heartbeats in HeartbeatReceiver.")
    val now = clock.getTimeMillis()

    /**
     * 遍历executorLastSeen映射
     */
    for ((executorId, lastSeenMs) <- executorLastSeen) {
      /**
       * 取出最后一次心跳的时间戳与当前对比，如果时间差值大于spark.network.timeout，就表示Executor已经超时
       */
      if (now - lastSeenMs > executorTimeoutMs) {
        logWarning(s"Removing executor $executorId with no recent heartbeats: " +
          s"${now - lastSeenMs} ms exceeds timeout $executorTimeoutMs ms")

        /**
         * 从调度体系中移除超时的Executor。
         */
        scheduler.executorLost(executorId, SlaveLost("Executor heartbeat " +
          s"timed out after ${now - lastSeenMs} ms"))
          // Asynchronously kill the executor to avoid blocking the current thread
        /**
         * 向killExecutorThread线程池提交执行SparkContext.killAndReplaceExecutor()方法的任务，
         * 异步地杀掉超时的Executor。
         * 通过异步线程“杀死”（kill）Executor，是为了不阻塞执行expireDeadHosts方法的线程。
         */
        killExecutorThread.submit(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError {
            // Note: we want to get an executor back after expiring this one,
            // so do not simply call `sc.killExecutor` here (SPARK-8119)
            sc.killAndReplaceExecutor(executorId)
          }
        })
        /**
         * 从executorLastSeen映射中删掉超时Executor ID的条目
         */
        executorLastSeen.remove(executorId)
      }
    }
  }

  override def onStop(): Unit = {
    if (timeoutCheckingTask != null) {
      timeoutCheckingTask.cancel(true)
    }
    eventLoopThread.shutdownNow()
    killExecutorThread.shutdownNow()
  }
}


private[spark] object HeartbeatReceiver {
  val ENDPOINT_NAME = "HeartbeatReceiver"
}
