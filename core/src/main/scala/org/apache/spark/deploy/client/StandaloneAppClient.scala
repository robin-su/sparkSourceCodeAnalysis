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

package org.apache.spark.deploy.client

import java.util.concurrent._
import java.util.concurrent.{Future => JFuture, ScheduledFuture => JScheduledFuture}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.deploy.{ApplicationDescription, ExecutorState}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.master.Master
import org.apache.spark.internal.Logging
import org.apache.spark.rpc._
import org.apache.spark.util.{RpcUtils, ThreadUtils}

/**
 * StandaloneAppClient是在Standalone模式下，Application与集群管理器进行对话的客户端
 *
 * Interface allowing applications to speak with a Spark standalone cluster manager.
 *
 * Takes a master URL, an app description, and a listener for cluster events, and calls
 * back the listener when various events occur.
 *
 * @param masterUrls Each url should look like spark://host:port.
 */
private[spark] class StandaloneAppClient(
    rpcEnv: RpcEnv, // 即SparkContext的SparkEnv的RpcEnv。
    masterUrls: Array[String], // 用于缓存每个Master的spark://host:port格式的URL的数组。
    appDescription: ApplicationDescription,//Application的描述信息（ApplicationDescription）。Application-Description中记录
    // 了Application的名称（name）、Application需要的最大内核数（maxCores）、每个Executor所需要的内存大小（memoryPerExecutorMB）、
    // 运行CoarseGrainedExecutorBackend进程的命令（command）、Spark UI的URL（appUiUrl）、事件日志的路径（eventLogDir）、事件日志
    // 采用的压缩算法名（eventLogCodec）、每个Executor所需的内核数（coresPerExecutor）、提交Application的用户名（user）等信息。
    listener: StandaloneAppClientListener, // 类型为StandaloneAppClientListener，是对集群事件的监听器。Standalone-AppClientListener
    // 有两个实现类，分别是StandaloneSchedulerBackend和AppClient-Collector。AppClientCollector只用于测试，StandaloneSchedulerBackend
    // 可用在local-cluster或Standalone模式下。

    conf: SparkConf)
  extends Logging {
  // Master的RPC地址（RpcAddress）。
  private val masterRpcAddresses = masterUrls.map(RpcAddress.fromSparkURL(_))
  //注册超时的秒数，固定为20。
  private val REGISTRATION_TIMEOUT_SECONDS = 20
  // 注册的重试次数，固定为3。
  private val REGISTRATION_RETRIES = 3
  // 类型为AtomicReference，用于持有ClientEndpoint的RpcEndpointRef。
  private val endpoint = new AtomicReference[RpcEndpointRef]
  // 类型为AtomicReference，用于持有Application的ID。
  private val appId = new AtomicReference[String]
  // 类型为AtomicReference，用于标识是否已经将Application注册到Master。
  private val registered = new AtomicBoolean(false)


  private class ClientEndpoint(override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint
    with Logging {
    // 处于激活状态的Master的RpcEndpointRef。
    private var master: Option[RpcEndpointRef] = None
    // To avoid calling listener.disconnected() multiple times
    // 是否已经与Master断开连接。此属性用于防止多次调用Stand-aloneAppClientListener的disconnected方法。
    private var alreadyDisconnected = false
    // To avoid calling listener.dead() multiple times
    /**
     * 表示ClientEndpoint是否已经“死掉”。此属性用于防止多次调用Stand-aloneAppClientListener的dead方法。
     */
    private val alreadyDead = new AtomicBoolean(false)
    /**
     * 用于保存registerMasterThreadPool执行的向各个Master注册Application的任务返回的Future。
     */
    private val registerMasterFutures = new AtomicReference[Array[JFuture[_]]]
    /**
     * 用于持有向registrationRetryThread提交关于注册的定时调度返回的ScheduledFuture。
     */
    private val registrationRetryTimer = new AtomicReference[JScheduledFuture[_]]

    // A thread pool for registering with masters. Because registering with a master is a blocking
    // action, this thread pool must be able to create "masterRpcAddresses.size" threads at the same
    // time so that we can register with all masters.
    /**
     * 用于向Master注册Application的ThreadPoolExecutor。registerMasterThreadPool的线程池大小为外部类
     * StandaloneAppClient的masterRpc-Addresses数组的大小，启动的线程以appclient-register-master-threadpool为前缀。
     */
    private val registerMasterThreadPool = ThreadUtils.newDaemonCachedThreadPool(
      "appclient-register-master-threadpool",
      masterRpcAddresses.length // Make sure we can register with all masters at the same time
    )

    // A scheduled executor for scheduling the registration actions
    /**
     * 类型为ScheduledExecutorService，用于对Application向Master进行注册的重试
     */
    private val registrationRetryThread =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("appclient-registration-retry-thread")

    /**
     * 将ClientEndpoint注册到RpcEnv的Dispatcher时，会触发对ClientEndpoint的onStart方法的调用。
     */
    override def onStart(): Unit = {
      try {
        // 向Master注册Application。
        registerWithMaster(1)
      } catch {
        case e: Exception =>
          logWarning("Failed to connect to master", e)
          markDisconnected()
          stop()
      }
    }

    /**
     *  Register with all masters asynchronously and returns an array `Future`s for cancellation.
     *
     *  tryRegisterAllMasters方法通过masterRpcAddresses中每个Master的RpcAddress，得到每个Master的RpcEndpointRef，
     *  然后通过这些RpcEndpointRef向每个Master发送RegisterApplication消息
     *
     */
    private def tryRegisterAllMasters(): Array[JFuture[_]] = {
      for (masterAddress <- masterRpcAddresses) yield {
        registerMasterThreadPool.submit(new Runnable {
          override def run(): Unit = try {
            if (registered.get) {
              return
            }
            logInfo("Connecting to master " + masterAddress.toSparkURL + "...")
            val masterRef = rpcEnv.setupEndpointRef(masterAddress, Master.ENDPOINT_NAME)
            masterRef.send(RegisterApplication(appDescription, self))
          } catch {
            case ie: InterruptedException => // Cancelled
            case NonFatal(e) => logWarning(s"Failed to connect to master $masterAddress", e)
          }
        })
      }
    }

    /**
     * 向registrationRetryThread提交定时调度，调度间隔时长为REGISTRATION_TIMEOUT_SECONDS。调度任务的执行逻辑是：如果已经注册成功，
     * 那么调用registerMasterFutures中保存的每一个Future的cancel方法取消向Master注册Application；如果重试次数超过了
     * REGISTRATION_RETRIES的限制，那么调用markDead方法标记当前ClientEndpoint进入死亡状态；
     * 其他情况下首先调用registerMasterFutures中保存的每一个Future的cancel方法取消向Master注册Application，
     * 然后再次调用registerWithMaster
     *
     * Register with all masters asynchronously. It will call `registerWithMaster` every
     * REGISTRATION_TIMEOUT_SECONDS seconds until exceeding REGISTRATION_RETRIES times.
     * Once we connect to a master successfully, all scheduling work and Futures will be cancelled.
     *
     * nthRetry means this is the nth attempt to register with master.
     */
    private def registerWithMaster(nthRetry: Int) {
      /**
       * 向所有的Master尝试注册Application
       */
      registerMasterFutures.set(tryRegisterAllMasters())
      registrationRetryTimer.set(registrationRetryThread.schedule(new Runnable { // 向所有的Master尝试注册
        override def run(): Unit = {
          if (registered.get) { // 如果已经注册成功过，那么取消向Master注册Application
            registerMasterFutures.get.foreach(_.cancel(true))
            registerMasterThreadPool.shutdownNow()
          } else if (nthRetry >= REGISTRATION_RETRIES) { // 重试次数超过限制，标记ClientEndpoint死亡
            markDead("All masters are unresponsive! Giving up.")
          } else {
            registerMasterFutures.get.foreach(_.cancel(true))
            // 向Master注册Application的重试
            registerWithMaster(nthRetry + 1)
          }
        }
      }, REGISTRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS))
    }

    /**
     * Send a message to the current master. If we have not yet registered successfully with any
     * master, the message will be dropped.
     */
    private def sendToMaster(message: Any): Unit = {
      master match {
        case Some(masterRef) => masterRef.send(message)
        case None => logWarning(s"Drop $message because has not yet connected to master")
      }
    }

    private def isPossibleMaster(remoteAddress: RpcAddress): Boolean = {
      masterRpcAddresses.contains(remoteAddress)
    }

    override def receive: PartialFunction[Any, Unit] = {
      case RegisteredApplication(appId_, masterRef) =>
        // FIXME How to handle the following cases?
        // 1. A master receives multiple registrations and sends back multiple
        // RegisteredApplications due to an unstable network.
        // 2. Receive multiple RegisteredApplication from different masters because the master is
        // changing.
        appId.set(appId_)
        registered.set(true)
        master = Some(masterRef)
        listener.connected(appId.get)

      case ApplicationRemoved(message) =>
        markDead("Master removed our application: %s".format(message))
        stop()

      case ExecutorAdded(id: Int, workerId: String, hostPort: String, cores: Int, memory: Int) =>
        val fullId = appId + "/" + id
        logInfo("Executor added: %s on %s (%s) with %d core(s)".format(fullId, workerId, hostPort,
          cores))
        listener.executorAdded(fullId, workerId, hostPort, cores, memory)

      case ExecutorUpdated(id, state, message, exitStatus, workerLost) =>
        val fullId = appId + "/" + id
        val messageText = message.map(s => " (" + s + ")").getOrElse("")
        logInfo("Executor updated: %s is now %s%s".format(fullId, state, messageText))
        if (ExecutorState.isFinished(state)) {
          listener.executorRemoved(fullId, message.getOrElse(""), exitStatus, workerLost)
        }

      case WorkerRemoved(id, host, message) =>
        logInfo("Master removed worker %s: %s".format(id, message))
        listener.workerRemoved(id, host, message)

      case MasterChanged(masterRef, masterWebUiUrl) =>
        logInfo("Master has changed, new master is at " + masterRef.address.toSparkURL)
        master = Some(masterRef)
        alreadyDisconnected = false
        masterRef.send(MasterChangeAcknowledged(appId.get))
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      /**
       * 接收到StopAppClient消息后，ClientEndpoint最重要的一步是向Master发送UnregisterApplication消息。对于请求Executor（
       * RequestExecutors）和“杀死”Executor（KillExecutors）两种消息，ClientEndpoint将通过调用askAndReplyAsync方法
       * 向Master转发这两种消息。
       */
      case StopAppClient =>
        markDead("Application has been stopped.")
        sendToMaster(UnregisterApplication(appId.get))
        context.reply(true)
        stop()

      /**
       * 对于请求Executor（RequestExecutors）和“杀死”Executor（KillExecutors）两种消息，ClientEndpoint将通过调用
       * askAndReplyAsync方法
       */
      case r: RequestExecutors =>
        master match {
          case Some(m) => askAndReplyAsync(m, context, r)
          case None =>
            logWarning("Attempted to request executors before registering with Master.")
            context.reply(false)
        }

      case k: KillExecutors =>
        master match {
          case Some(m) => askAndReplyAsync(m, context, k)
          case None =>
            logWarning("Attempted to kill executors before registering with Master.")
            context.reply(false)
        }
    }

    private def askAndReplyAsync[T](
        endpointRef: RpcEndpointRef,
        context: RpcCallContext,
        msg: T): Unit = {
      // Ask a message and create a thread to reply with the result.  Allow thread to be
      // interrupted during shutdown, otherwise context must be notified of NonFatal errors.
      endpointRef.ask[Boolean](msg).andThen {
        case Success(b) => context.reply(b)
        case Failure(ie: InterruptedException) => // Cancelled
        case Failure(NonFatal(t)) => context.sendFailure(t)
      }(ThreadUtils.sameThread)
    }

    override def onDisconnected(address: RpcAddress): Unit = {
      if (master.exists(_.address == address)) {
        logWarning(s"Connection to $address failed; waiting for master to reconnect...")
        markDisconnected()
      }
    }

    override def onNetworkError(cause: Throwable, address: RpcAddress): Unit = {
      if (isPossibleMaster(address)) {
        logWarning(s"Could not connect to $address: $cause")
      }
    }

    /**
     * 将当前ClientEndpoint标记为和Master断开连接，并且调用StandaloneAppClient的stop方法
     *
     * Notify the listener that we disconnected, if we hadn't already done so before.
     */
    def markDisconnected() {
      // markDisconnected方法在判断ClientEndpoint的alreadyDisconnected状态不为true时
      if (!alreadyDisconnected) {
        // 先调用外部类StandaloneAppClient的listener属性的disconnected方法
        listener.disconnected()
        // 然后将alreadyDisconnected设置为true。
        alreadyDisconnected = true
      }
    }

    def markDead(reason: String) {
      if (!alreadyDead.get) {
        listener.dead(reason)
        alreadyDead.set(true)
      }
    }

    override def onStop(): Unit = {
      if (registrationRetryTimer.get != null) {
        registrationRetryTimer.get.cancel(true)
      }
      registrationRetryThread.shutdownNow()
      registerMasterFutures.get.foreach(_.cancel(true))
      registerMasterThreadPool.shutdownNow()
    }

  }

  /**
   * 在启动StandaloneAppClient时只做了一件事——向SparkContext的SparkEnv的RpcEnv注册ClientEndpoint，
   * 进而引起对ClientEndpoint的启动和向Master注册Application。
   */
  def start() {
    // Just launch an rpcEndpoint; it will call back into the listener.
    endpoint.set(rpcEnv.setupEndpoint("AppClient", new ClientEndpoint(rpcEnv)))
  }

  /**
   * 停止StandaloneAppClient实际不过是向ClientEnd-point发送了StopAppClient消息，
   * 并将ClientEndpoint的RpcEndpointRef从endpoint中清除。
   */
  def stop() {
    if (endpoint.get != null) {
      try {
        val timeout = RpcUtils.askRpcTimeout(conf)
        timeout.awaitResult(endpoint.get.ask[Boolean](StopAppClient))
      } catch {
        case e: TimeoutException =>
          logInfo("Stop request to Master timed out; it may already be shut down.")
      }
      endpoint.set(null)
    }
  }

  /**
   * 用于向Master请求所需的所有Executor资源。
   *
   * requestTotalExecutors方法只能在Application注册成功之后调用。requestTotalExecutors方法将向ClientEndpoint发送RequestExecutors消息
   * （此消息携带Application ID和所需的Executor总数）。根据对ClientEndpoint处理RequestExecutors消息的分析，我们知道ClientEndpoint将向
   * Master转发RequestExecutors消息。
   *
   * Request executors from the Master by specifying the total number desired,
   * including existing pending and running executors.
   *
   * @return whether the request is acknowledged.
   */
  def requestTotalExecutors(requestedTotal: Int): Future[Boolean] = {
    if (endpoint.get != null && appId.get != null) {
      endpoint.get.ask[Boolean](RequestExecutors(appId.get, requestedTotal))
    } else {
      logWarning("Attempted to request executors before driver fully initialized.")
      Future.successful(false)
    }
  }

  /**
   * 用于向Master请求“杀死”Executor。
   *
   * Kill the given list of executors through the Master.
   * @return whether the kill request is acknowledged.
   */
  def killExecutors(executorIds: Seq[String]): Future[Boolean] = {
    if (endpoint.get != null && appId.get != null) {
      endpoint.get.ask[Boolean](KillExecutors(appId.get, executorIds))
    } else {
      logWarning("Attempted to kill executors before driver fully initialized.")
      Future.successful(false)
    }
  }

}
