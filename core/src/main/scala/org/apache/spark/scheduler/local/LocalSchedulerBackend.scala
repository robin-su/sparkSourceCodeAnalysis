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

package org.apache.spark.scheduler.local

import java.io.File
import java.net.URL
import java.nio.ByteBuffer

import org.apache.spark.{SparkConf, SparkContext, SparkEnv, TaskState}
import org.apache.spark.TaskState.TaskState
import org.apache.spark.executor.{Executor, ExecutorBackend}
import org.apache.spark.internal.Logging
import org.apache.spark.launcher.{LauncherBackend, SparkAppHandle}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo

private case class ReviveOffers()

private case class StatusUpdate(taskId: Long, state: TaskState, serializedData: ByteBuffer)

private case class KillTask(taskId: Long, interruptThread: Boolean, reason: String)

private case class StopExecutor()

/**
 * Calls to [[LocalSchedulerBackend]] are all serialized through LocalEndpoint. Using an
 * RpcEndpoint makes the calls on [[LocalSchedulerBackend]] asynchronous, which is necessary
 * to prevent deadlock between [[LocalSchedulerBackend]] and the [[TaskSchedulerImpl]].
 */
private[spark] class LocalEndpoint(
    override val rpcEnv: RpcEnv,
    userClassPath: Seq[URL], // 用户指定的ClassPath。
    scheduler: TaskSchedulerImpl, // 即Driver中的TaskSchedulerImpl。
    executorBackend: LocalSchedulerBackend, // 与LocalEndpoint相关联的LocalSchedulerBackend。
    private val totalCores: Int) // 用于执行任务的CPU内核总数。local模式下，totalCores固定为1。
  extends ThreadSafeRpcEndpoint with Logging {

  private var freeCores = totalCores // 空闲的CPU内核数。应用程序提交的Task正式运行之前，freeCores与totalCores相等。

  // local部署模式下，与Driver处于同一JVM进程的Executor的身份标识。由于LocalEndpoint只在local模式中使用，因此localExecutorId固定为driver。
  val localExecutorId = SparkContext.DRIVER_IDENTIFIER
  // 与Driver处于同一JVM进程的Executor所在的Host。由于LocalEndpoint只在local模式中使用，因此localExecutorHostname固定为localhost。
  val localExecutorHostname = "localhost"

  //与Driver处于同一JVM进程的Executor。由于LocalEndpoint的totalCores等于1，因此应用本地有且只有一个Executor，且此Executor在LocalEndpoint构造的过程中就已经实例化。
  private val executor = new Executor(
    localExecutorId, localExecutorHostname, SparkEnv.get, userClassPath, isLocal = true)

  override def receive: PartialFunction[Any, Unit] = {
    case ReviveOffers =>
      reviveOffers()

    case StatusUpdate(taskId, state, serializedData) =>
      scheduler.statusUpdate(taskId, state, serializedData)
      if (TaskState.isFinished(state)) {
        freeCores += scheduler.CPUS_PER_TASK
        reviveOffers()
      }

    case KillTask(taskId, interruptThread, reason) =>
      executor.killTask(taskId, interruptThread, reason)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case StopExecutor =>
      executor.stop()
      context.reply(true)
  }

  def reviveOffers() {
    // 创建只包含一个WorkerOffer（理解为“就业机会”似乎更加生动，由于totalCores为1，因此只有一个职位）的序列
    val offers = IndexedSeq(new WorkerOffer(localExecutorId, localExecutorHostname, freeCores,
      Some(rpcEnv.address.hostPort)))
    for (task <- scheduler.resourceOffers(offers).flatten) { // 给Task分配资源
      // 将空闲的CPU内核数freeCores减去1
      freeCores -= scheduler.CPUS_PER_TASK
      executor.launchTask(executorBackend, task) // 运行task
    }
  }
}

/**
 * LocalSchedulerBackend:local模式中的调度后端接口。
 * 在local模式下，Executor、LocalSchedulerBackend、Driver都运行在同一个JVM进程中。
 *
 * Used when running a local version of Spark where the executor, backend, and master all run in
 * the same JVM. It sits behind a [[TaskSchedulerImpl]] and handles launching tasks on a single
 * Executor (created by the [[LocalSchedulerBackend]]) running locally.
 */
private[spark] class LocalSchedulerBackend(
    conf: SparkConf,
    scheduler: TaskSchedulerImpl,
    val totalCores: Int)
  extends SchedulerBackend with ExecutorBackend with Logging {
//  当前应用程序的身份标识。local模式下的appId以local-为前缀，以系统当前时间戳为后缀，local模式下应用程序的身份标识是在构造LocalSchedulerBackend的时候生成的。
  private val appId = "local-" + System.currentTimeMillis
  // 即SparkContext中创建的LiveListenerBus
  private var localEndpoint: RpcEndpointRef = null
  private val userClassPath = getUserClasspath(conf) // 用户指定的类路径。可以通过spark.executor.extraClassPath属性进行配置，配置时可以用英文逗号分隔多个类路径。
  private val listenerBus = scheduler.sc.listenerBus // 即SparkContext中创建的LiveListenerBus
  // LauncherBackend的匿名实现类的实例。此匿名实现类实现了LauncherBackend的onStopRequest方法，用于停止Executor、
  // 将launcherBackend的状态标记为KILLED、关闭launcherBackend与LauncherServer之间的Socket连接
  private val launcherBackend = new LauncherBackend() {
    override def conf: SparkConf = LocalSchedulerBackend.this.conf
    override def onStopRequest(): Unit = stop(SparkAppHandle.State.KILLED)
  }

  /**
   * Returns a list of URLs representing the user classpath.
   *
   * @param conf Spark configuration.
   */
  def getUserClasspath(conf: SparkConf): Seq[URL] = {
    val userClassPathStr = conf.getOption("spark.executor.extraClassPath")
    userClassPathStr.map(_.split(File.pathSeparator)).toSeq.flatten.map(new File(_).toURI.toURL)
  }

  launcherBackend.connect()

  override def start() {
    val rpcEnv = SparkEnv.get.rpcEnv
    val executorEndpoint = new LocalEndpoint(rpcEnv, userClassPath, scheduler, this, totalCores)
    // 向LiveListenerBus投递SparkListenerExecutorAdded事件。local模式下SparkListener
    // ExecutorAdded事件携带的time为系统当前时间，executorId为driver, executorInfo（即ExecutorInfo, ExecutorInfo的executorHost属性为localhost, totalCores为1, logUrlMap为空）。
    localEndpoint = rpcEnv.setupEndpoint("LocalSchedulerBackendEndpoint", executorEndpoint)
    listenerBus.post(SparkListenerExecutorAdded(
      System.currentTimeMillis,
      executorEndpoint.localExecutorId,
      new ExecutorInfo(executorEndpoint.localExecutorHostname, totalCores, Map.empty)))
    // 调用LauncherBackend的setAppId方法向LauncherServer发送SetAppId消息。
    launcherBackend.setAppId(appId)
    // 调用LauncherBackend的setAppId方法向LauncherServer发送SetAppId消息。
    launcherBackend.setState(SparkAppHandle.State.RUNNING)
  }

  override def stop() {
    stop(SparkAppHandle.State.FINISHED)
  }

  override def reviveOffers() {
    localEndpoint.send(ReviveOffers)
  }

  override def defaultParallelism(): Int =
    scheduler.conf.getInt("spark.default.parallelism", totalCores)

  override def killTask(
      taskId: Long, executorId: String, interruptThread: Boolean, reason: String) {
    localEndpoint.send(KillTask(taskId, interruptThread, reason))
  }

  override def statusUpdate(taskId: Long, state: TaskState, serializedData: ByteBuffer) {
    localEndpoint.send(StatusUpdate(taskId, state, serializedData))
  }

  override def applicationId(): String = appId

  override def maxNumConcurrentTasks(): Int = totalCores / scheduler.CPUS_PER_TASK

  private def stop(finalState: SparkAppHandle.State): Unit = {
    localEndpoint.ask(StopExecutor)
    try {
      launcherBackend.setState(finalState)
    } finally {
      launcherBackend.close()
    }
  }

}
