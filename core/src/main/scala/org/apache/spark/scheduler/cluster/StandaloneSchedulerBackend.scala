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

package org.apache.spark.scheduler.cluster

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.Future

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.deploy.{ApplicationDescription, Command}
import org.apache.spark.deploy.client.{StandaloneAppClient, StandaloneAppClientListener}
import org.apache.spark.internal.Logging
import org.apache.spark.launcher.{LauncherBackend, SparkAppHandle}
import org.apache.spark.rpc.RpcEndpointAddress
import org.apache.spark.scheduler._
import org.apache.spark.util.Utils

/**
 * A [[SchedulerBackend]] implementation for Spark's standalone cluster manager.
 */
private[spark] class StandaloneSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext,
    masters: Array[String])
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv)
  with StandaloneAppClientListener
  with Logging {

  private var client: StandaloneAppClient = null
  /**
   * 标记StandaloneSchedulerBackend是否正在停止。
   */
  private val stopping = new AtomicBoolean(false)
  private val launcherBackend = new LauncherBackend() {
    override protected def conf: SparkConf = sc.conf
    override protected def onStopRequest(): Unit = stop(SparkAppHandle.State.KILLED)
  }

  @volatile var shutdownCallback: StandaloneSchedulerBackend => Unit = _
  /**
   * Application的ID。
   */
  @volatile private var appId: String = _

  /**
   * 使用Java的信号量（Semaphore）实现的栅栏，用于等待Application向Master注册完成后，将Application的当前状态
   * （此时为正在运行，即RUNN-ING）告知LauncherServer。
   */
  private val registrationBarrier = new Semaphore(0)

  /**
   * Application可以申请获得的最大内核数。可通过spark.cores.max属性配置。
   */
  private val maxCores = conf.getOption("spark.cores.max").map(_.toInt)
  /**
   * Application期望获得的内核数，如果设置了maxCores，则为max-Cores，否则为0。
   */
  private val totalExpectedCores = maxCores.getOrElse(0)

  override def start() {
    super.start() // 创建并注册DriverEndpoint

    // SPARK-21159. The scheduler backend should only try to connect to the launcher when in client
    // mode. In cluster mode, the code that submits the application to the Master needs to connect
    // to the launcher instead.
    if (sc.deployMode == "client") {
      // 与LauncherServer建立连接
      launcherBackend.connect()
    }

    // The endpoint for executors to talk to us
    /**
     * 生成Driver URL，格式为spark://CoarseGrainedScheduler@${driverHost}:${driverPort}。Executor将通过此URL与Driver通信。
     */
    val driverUrl = RpcEndpointAddress(
      sc.conf.get("spark.driver.host"),
      sc.conf.get("spark.driver.port").toInt,
      CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString
    /**
     * 拼接参数列表args
     */
    val args = Seq(
      "--driver-url", driverUrl,
      "--executor-id", "{{EXECUTOR_ID}}",
      "--hostname", "{{HOSTNAME}}",
      "--cores", "{{CORES}}",
      "--app-id", "{{APP_ID}}",
      "--worker-url", "{{WORKER_URL}}")
    /**
     * 获取额外的Java参数extraJavaOpts（通过spark.executor.extraJavaOptions属性配置）
     */
    val extraJavaOpts = sc.conf.getOption("spark.executor.extraJavaOptions")
      .map(Utils.splitCommandString).getOrElse(Seq.empty)
    /**
     * 额外的类路径classPathEntries（通过spark.executor.extraClassPath属性配置）
     */
    val classPathEntries = sc.conf.getOption("spark.executor.extraClassPath")
      .map(_.split(java.io.File.pathSeparator).toSeq).getOrElse(Nil)
    /**
     * 额外的库路径libraryPathEntries（通过spark.executor.extraLibraryPath属性配置）等
     */
    val libraryPathEntries = sc.conf.getOption("spark.executor.extraLibraryPath")
      .map(_.split(java.io.File.pathSeparator).toSeq).getOrElse(Nil)

    // When testing, expose the parent class path to the child. This is processed by
    // compute-classpath.{cmd,sh} and makes all needed jars available to child processes
    // when the assembly is built with the "*-provided" profiles enabled.
    val testingClassPath =
      if (sys.props.contains("spark.testing")) {
        sys.props("java.class.path").split(java.io.File.pathSeparator).toSeq
      } else {
        Nil
      }

    // Start executors with a few necessary configs for registering with the scheduler
    /**
     * 从SparkConf中获取需要传递给Executor用于启动的配置sparkJavaOpts。这些配置包括：以spark.auth开头的配置但不包括spark.authenticate.secret）、
     * 以spark.ssl开头的配置、
     * 以spark.rpc开头的配置、
     * 以spark．
     * 开头且以．port结尾的配置
     * 及以spark.port．开头的配置。
     */
    val sparkJavaOpts = Utils.sparkJavaOpts(conf, SparkConf.isExecutorStartupConf)
    //将extraJavaOpts与sparkJavaOpts合并到javaOpts。
    val javaOpts = sparkJavaOpts ++ extraJavaOpts
    /**
     * 创建Command对象。样例类Command定义了执行Executor的命令。这里以
     * org. apache.spark.executor.CoarseGrainedExecutorBackend作为Command的mainClass属性、
     * 以args作为Command的arguments属性、
     * 以SparkContext的executorEnvs属性作为Command的environment属性、
     * 以classPathEntries作为Command的classPathEntries属性、
     * 以library-PathEntries作为Command的libraryPathEntries属性、
     * 以javaOpts作为Command的javaOpts属性。
     */
    val command = Command("org.apache.spark.executor.CoarseGrainedExecutorBackend",
      args, sc.executorEnvs, classPathEntries ++ testingClassPath, libraryPathEntries, javaOpts)
    // 获取Spark UI的http地址appUIAddress
    val webUrl = sc.ui.map(_.webUrl).getOrElse("")
    // 获取每个Executor分配的内核数cores-PerExecutor（可通过spark.executor.cores属性配置）
    val coresPerExecutor = conf.getOption("spark.executor.cores").map(_.toInt)
    // If we're using dynamic allocation, set our initial executor limit to 0 for now.
    // ExecutorAllocationManager will send the real initial limit to the Master later.
    /**
     * Executor的初始限制initialExecutor-Limit（如果启用了动态分配Executor，那么initialExecutorLimit被设置为0,
     * ExecutorAllocationManager之后会将真实的初始限制值传递给Master）
     */
    val initialExecutorLimit =
      if (Utils.isDynamicAllocationEnabled(conf)) {
        Some(0)
      } else {
        None
      }
    // 创建Application描述信息（ApplicationDescription）
    val appDesc = ApplicationDescription(sc.appName, maxCores, sc.executorMemory, command,
      webUrl, sc.eventLogDir, sc.eventLogCodec, coresPerExecutor, initialExecutorLimit)
    // 创建并启动StandaloneAppClient
    client = new StandaloneAppClient(sc.env.rpcEnv, masters, appDesc, this, conf)

    /**
     * StandaloneAppClient的start方法将创建ClientEndpoint，并向SparkContext的SparkEnv的RpcEnv注册ClientEndpoint，
     * 进而引起对ClientEndpoint的启动和向Master注册Application。
     */
    client.start()
    // 向LauncherServer传递应用已经提交（SUBMITTED）的状态。
    launcherBackend.setState(SparkAppHandle.State.SUBMITTED)

    /**
     * 我们知道当注册Application成功后，Master将向ClientEndpoint发送RegisteredApplication消息，进而调用
     * StandaloneSchedulerBackend的connected方法释放信号量，这样waitForRegistration方法将可以获得信号量。
     */
    waitForRegistration()
    // 向LauncherServer传递应用正在运行（RUNNING）的状态。
    launcherBackend.setState(SparkAppHandle.State.RUNNING)
  }

  /**
   * StandaloneSchedulerBackend的stop方法用于停止StandaloneSchedulerBackend
   */
  override def stop(): Unit = {
    stop(SparkAppHandle.State.FINISHED)
  }

  override def connected(appId: String) {
    logInfo("Connected to Spark cluster with app ID " + appId)
    this.appId = appId
    notifyContext()
    launcherBackend.setAppId(appId)
  }

  override def disconnected() {
    notifyContext()
    if (!stopping.get) {
      logWarning("Disconnected from Spark cluster! Waiting for reconnection...")
    }
  }

  override def dead(reason: String) {
    notifyContext()
    if (!stopping.get) {
      launcherBackend.setState(SparkAppHandle.State.KILLED)
      logError("Application has been killed. Reason: " + reason)
      try {
        scheduler.error(reason)
      } finally {
        // Ensure the application terminates, as we can no longer run jobs.
        sc.stopInNewThread()
      }
    }
  }

  override def executorAdded(fullId: String, workerId: String, hostPort: String, cores: Int,
    memory: Int) {
    logInfo("Granted executor ID %s on hostPort %s with %d core(s), %s RAM".format(
      fullId, hostPort, cores, Utils.megabytesToString(memory)))
  }

  override def executorRemoved(
      fullId: String, message: String, exitStatus: Option[Int], workerLost: Boolean) {
    val reason: ExecutorLossReason = exitStatus match {
      case Some(code) => ExecutorExited(code, exitCausedByApp = true, message)
      case None => SlaveLost(message, workerLost = workerLost)
    }
    logInfo("Executor %s removed: %s".format(fullId, message))
    removeExecutor(fullId.split("/")(1), reason)
  }

  override def workerRemoved(workerId: String, host: String, message: String): Unit = {
    logInfo("Worker %s removed: %s".format(workerId, message))
    removeWorker(workerId, host, message)
  }

  override def sufficientResourcesRegistered(): Boolean = {
    totalCoreCount.get() >= totalExpectedCores * minRegisteredRatio
  }

  override def applicationId(): String =
    Option(appId).getOrElse {
      logWarning("Application ID is not initialized yet.")
      super.applicationId
    }

  /**
   * Request executors from the Master by specifying the total number desired,
   * including existing pending and running executors.
   *
   * @return whether the request is acknowledged.
   */
  protected override def doRequestTotalExecutors(requestedTotal: Int): Future[Boolean] = {
    Option(client) match {
      case Some(c) => c.requestTotalExecutors(requestedTotal)
      case None =>
        logWarning("Attempted to request executors before driver fully initialized.")
        Future.successful(false)
    }
  }

  /**
   * Kill the given list of executors through the Master.
   * @return whether the kill request is acknowledged.
   */
  protected override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = {
    Option(client) match {
      case Some(c) => c.killExecutors(executorIds)
      case None =>
        logWarning("Attempted to kill executors before driver fully initialized.")
        Future.successful(false)
    }
  }

  private def waitForRegistration() = {
    registrationBarrier.acquire()
  }

  private def notifyContext() = {
    registrationBarrier.release()
  }

  /**
   * 停止StandaloneAppClient，然后调用父类CoarseGrainedSchedulerBackend的stop方法，最后回调关闭函数
   *
   * @param finalState
   */
  private def stop(finalState: SparkAppHandle.State): Unit = {
    if (stopping.compareAndSet(false, true)) {
      try {
        super.stop()
        if (client != null) {
          client.stop()
        }
        val callback = shutdownCallback
        if (callback != null) {
          callback(this)
        }
      } finally {
        launcherBackend.setState(finalState)
        launcherBackend.close()
      }
    }
  }

}
