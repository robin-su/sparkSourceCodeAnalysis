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

package org.apache.spark.deploy.master

import java.text.SimpleDateFormat
import java.util.{Date, Locale}
import java.util.concurrent.{ScheduledFuture, TimeUnit}

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.util.Random

import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.{ApplicationDescription, DriverDescription,
  ExecutorState, SparkHadoopUtil}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.deploy.master.MasterMessages._
import org.apache.spark.deploy.master.ui.MasterWebUI
import org.apache.spark.deploy.rest.StandaloneRestServer
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.rpc._
import org.apache.spark.serializer.{JavaSerializer, Serializer}
import org.apache.spark.util.{SparkUncaughtExceptionHandler, ThreadUtils, Utils}

/**
 * Master的职责包括Worker的管理、Application的管理、Driver的管理等。Master负责对整个集群中所有资源的统一管理和分配，它接收各个Worker的注册、
 * 更新状态、心跳等消息，也接收Driver和Application的注册.Worker向Master注册时会携带自身的身份和资源信息（如ID、host、port、内核数、内存大小
 * 等），这些资源将按照一定的资源调度策略分配给Driver或Application。Master给Driver分配了资源后，将向Worker发送启动Driver的命令，后者在接收
 * 到启动Driver的命令后启动Driver。Master给Application分配了资源后，将向Worker发送启动Executor的命令，后者在接收到启动Executor的命令后
 * 启动Executor。
 *
 * Master接收Worker的状态更新消息，用于“杀死”不匹配的Driver或Application。
 *
 * Worker向Master发送的心跳消息有两个目的：一是告知Master自己还“活着”，另外则是某个Master出现故障后，通过领导选举选择了其他Master负责对整个
 * 集群的管理，此时被激活的Master可能并没有缓存Worker的相关信息，因此需要告知Worker重新向新的Master注册。
 *
 *
 * Master接收到Driver提交的应用程序后，需要根据应用的资源需求，将应用分配到Worker上去运行。一个集群刚开始的时候只有Master，为了让后续启动的
 * Worker加入到Master的集群中，每个Worker都需要在启动的时候向Master注册，Master接收到Worker的注册信息后，将把Worker的各种重要信息（如ID、
 * host、port、内核数、内存大小等信息）缓存起来，以便进行资源的分配与调度。Master为了容灾，还将Worker的信息通过持久化引擎进行持久化，以便经过
 * 领导选举出的新Master能够将集群的状态从错误或灾难中恢复。
 *
 * 在Spark集群中，Master接收到Driver提交的应用程序后，需要根据应用的资源需求，将应用分配到Worker上去运行。一个集群刚开始的时候只有Master，
 * 为了让后续启动的Worker加入到Master的集群中，每个Worker都需要在启动的时候向Master注册，Master接收到Worker的注册信息后，将把Worker的
 * 各种重要信息（如ID、host、port、内核数、内存大小等信息）缓存起来，以便进行资源的分配与调度。Master为了容灾，还将Worker的信息通过持久化引
 * 擎进行持久化，以便经过领导选举出的新Master能够将集群的状态从错误或灾难中恢复。
 *
 * @param rpcEnv
 * @param address
 * @param webUiPort
 * @param securityMgr
 * @param conf
 */
private[deploy] class Master(
    override val rpcEnv: RpcEnv,
    address: RpcAddress,
    webUiPort: Int,
    val securityMgr: SecurityManager,
    val conf: SparkConf)
  extends ThreadSafeRpcEndpoint with Logging with LeaderElectable {

//  包含一个线程的ScheduledThreadPoolExecutor，启动的线程以master-forward-message-thread作为名称。forwardMessageThread主要用于运行
//  checkForWorkerTimeOutTask和recoveryCompletionTask。
  private val forwardMessageThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-forward-message-thread")
//  加载hadoopconf配置
  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)

// For application IDs
//  设置appID
  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US)
// worker 连接超时时间，默认60秒
  private val WORKER_TIMEOUT_MS = conf.getLong("spark.worker.timeout", 60) * 1000
//  执行完成的app在webUI里保存的数量，默认是200
  private val RETAINED_APPLICATIONS = conf.getInt("spark.deploy.retainedApplications", 200)
// 保存执行的driver数量，默认是200
  private val RETAINED_DRIVERS = conf.getInt("spark.deploy.retainedDrivers", 200)
// 从workers中移除处于死亡（DEAD）状态的Worker所对应的WorkerInfo的权重。可通过spark.dead.worker.persistence属性配置，默认为15。
  private val REAPER_ITERATIONS = conf.getInt("spark.dead.worker.persistence", 15)
// 恢复模式。可通过spark.deploy.recoveryMode属性配置，默认为NONE。
  private val RECOVERY_MODE = conf.get("spark.deploy.recoveryMode", "NONE")
// Executor的最大重试数。可通过spark.deploy.max-ExecutorRetries属性配置，默认为10。
  private val MAX_EXECUTOR_RETRIES = conf.getInt("spark.deploy.maxExecutorRetries", 10)
// 所有注册到Master的Worker信息（WorkerInfo）的集合
  val workers = new HashSet[WorkerInfo]
// Application ID与ApplicationInfo的映射关系。
  val idToApp = new HashMap[String, ApplicationInfo]
//  正等待调度的Application所对应的ApplicationInfo的集合。
  private val waitingApps = new ArrayBuffer[ApplicationInfo]
//  所有ApplicationInfo的集合。
  val apps = new HashSet[ApplicationInfo]
// Worker id与WorkerInfo的映射关系。
  private val idToWorker = new HashMap[String, WorkerInfo]
//  Worker的RpcEnv的地址（RpcAddress）与WorkerInfo的映射关系。
  private val addressToWorker = new HashMap[RpcAddress, WorkerInfo]
// RpcEndpointRef与ApplicationInfo的映射关系
  private val endpointToApp = new HashMap[RpcEndpointRef, ApplicationInfo]
//  RpcEndpointRef与ApplicationInfo的映射关系。
  private val addressToApp = new HashMap[RpcAddress, ApplicationInfo]
// RpcEndpointRef与ApplicationInfo的映射关系。
  private val completedApps = new ArrayBuffer[ApplicationInfo]
  private var nextAppNumber = 0
// 所有Driver信息（DriverInfo）的集合。
  private val drivers = new HashSet[DriverInfo]
  // 已经完成的DriverInfo的集合。
  private val completedDrivers = new ArrayBuffer[DriverInfo]
  // 正等待调度的Driver所对应的DriverInfo的集合。
  private val waitingDrivers = new ArrayBuffer[DriverInfo]
  // 下一个Driver的号码。
  private var nextDriverNumber = 0
//  检查host
  Utils.checkHost(address.host)
//  assert(host != null && host.indexOf(':') == -1, s"Expected hostname (not IP) but got $host")

//  master注册Metrics服务
  private val masterMetricsSystem = MetricsSystem.createMetricsSystem("master", conf, securityMgr)
//  app注册Metrics服务
  private val applicationMetricsSystem = MetricsSystem.createMetricsSystem("applications", conf,
    securityMgr)
// 类型为MasterSource，是有关Master的度量来源。
  private val masterSource = new MasterSource(this)

// After onStart, webUi will be set
//  Master的WebUI，类型为WebUI的子类MasterWebUI。
  private var webUi: MasterWebUI = null
// master的公共访问地址
  private val masterPublicAddress = {
    val envVar = conf.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else address.host
  }
// Master的Spark URL（即spark://host:port格式的地址）。
  private val masterUrl = address.toSparkURL
  // Master的WebUI的URL。
  private var masterWebUiUrl: String = _

  // Master所处的状态。Master的状态包括支持（STANDBY）、激活（ALIVE）、恢复中（RECOVERING）、完成恢复（COMPLETING_RECOVERY）等。
  private var state = RecoveryState.STANDBY
  // 持久化引擎
  private var persistenceEngine: PersistenceEngine = _
//  领导选举代理（LeaderElectionAgent）
  private var leaderElectionAgent: LeaderElectionAgent = _
  // 当Master被选举为领导后，用于集群状态恢复的任务。
  private var recoveryCompletionTask: ScheduledFuture[_] = _
  // 超时的test检查
  private var checkForWorkerTimeOutTask: ScheduledFuture[_] = _

  // As a temporary workaround before better ways of configuring memory, we allow users to set
  // a flag that will perform round-robin scheduling across the nodes (spreading out each app
  // among all the nodes) instead of trying to consolidate each app onto a small # of nodes.
  // 开启内存分配之前的临时解决方案,将app发送到多节点进行计算
  private val spreadOutApps = conf.getBoolean("spark.deploy.spreadOut", true)

  // Default maxCores for applications that don't specify it (i.e. pass Int.MaxValue)
//  未指定参数的app,默认使用最大cores进行计算
  private val defaultCores = conf.getInt("spark.deploy.defaultCores", Int.MaxValue)
//  是否使用反向代理,默认关闭
  val reverseProxy = conf.getBoolean("spark.ui.reverseProxy", false)
  if (defaultCores < 1) {
    throw new SparkException("spark.deploy.defaultCores must be positive")
  }

  // Alternative application submission gateway that is stable across Spark versions
//  备用app提交网关
//  开启rest服务,默认开启
  private val restServerEnabled = conf.getBoolean("spark.master.rest.enabled", false)
  // REST服务的实例，类型为StandaloneRestServer。
  private var restServer: Option[StandaloneRestServer] = None
  // REST服务绑定的端口。
  private var restServerBoundPort: Option[Int] = None

  {
    val authKey = SecurityManager.SPARK_AUTH_SECRET_CONF
    require(conf.getOption(authKey).isEmpty || !restServerEnabled,
      s"The RestSubmissionServer does not support authentication via ${authKey}.  Either turn " +
        "off the RestSubmissionServer with spark.master.rest.enabled=false, or do not use " +
        "authentication.")
  }
//  启动前将所有信息准备就绪
  override def onStart(): Unit = {
    logInfo("Starting Spark master at " + masterUrl)
    logInfo(s"Running Spark version ${org.apache.spark.SPARK_VERSION}")
    webUi = new MasterWebUI(this, webUiPort) // 创建Master的Web UI
//    启动web ui，其实部署在jetty容器中
    webUi.bind()
    masterWebUiUrl = "http://" + masterPublicAddress + ":" + webUi.boundPort
    if (reverseProxy) {
      masterWebUiUrl = conf.get("spark.ui.reverseProxyUrl", masterWebUiUrl)
      webUi.addProxy()
      logInfo(s"Spark Master is acting as a reverse proxy. Master, Workers and " +
       s"Applications UIs are available at $masterWebUiUrl")
    }
    //超时检查
    // 按照固定的频率去启动线程来检查 Worker 是否超时. 其实就是给自己发信息: CheckForWorkerTimeOut
    // 默认是每分钟检查一次.
    checkForWorkerTimeOutTask = forwardMessageThread.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        self.send(CheckForWorkerTimeOut)
      }
    }, 0, WORKER_TIMEOUT_MS, TimeUnit.MILLISECONDS)

    // rest信息
    if (restServerEnabled) {
      val port = conf.getInt("spark.master.rest.port", 6066)
      restServer = Some(new StandaloneRestServer(address.host, port, conf, self, masterUrl))
    }
    restServerBoundPort = restServer.map(_.start())
    // 注册MetricSystem测量系统
    masterMetricsSystem.registerSource(masterSource)
    masterMetricsSystem.start()
    applicationMetricsSystem.start()
    // Attach the master and app metrics servlet handler to the web ui after the metrics systems are
    // started.
//    将MetricSystem测量系统添加到webUI
    masterMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)
    applicationMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)
    // 序列化配置信息
    val serializer = new JavaSerializer(conf)
    // 匹配持久化方式ZOOKEEPER、FILESYSTEM、CUSTOM
    val (persistenceEngine_, leaderElectionAgent_) = RECOVERY_MODE match {
      case "ZOOKEEPER" =>
        logInfo("Persisting recovery state to ZooKeeper")
        val zkFactory =
          new ZooKeeperRecoveryModeFactory(conf, serializer)
        (zkFactory.createPersistenceEngine(), zkFactory.createLeaderElectionAgent(this))
      case "FILESYSTEM" =>
        val fsFactory =
          new FileSystemRecoveryModeFactory(conf, serializer)
        (fsFactory.createPersistenceEngine(), fsFactory.createLeaderElectionAgent(this))
      case "CUSTOM" =>
        val clazz = Utils.classForName(conf.get("spark.deploy.recoveryMode.factory"))
        val factory = clazz.getConstructor(classOf[SparkConf], classOf[Serializer])
          .newInstance(conf, serializer)
          .asInstanceOf[StandaloneRecoveryModeFactory]
        (factory.createPersistenceEngine(), factory.createLeaderElectionAgent(this))
      case _ =>
      // 这里调用了选主机制
        (new BlackHolePersistenceEngine(), new MonarchyLeaderAgent(this))
    }
    persistenceEngine = persistenceEngine_
    leaderElectionAgent = leaderElectionAgent_
  }

  override def onStop() {
    masterMetricsSystem.report()
    applicationMetricsSystem.report()
    // prevent the CompleteRecovery message sending to restarted master
    if (recoveryCompletionTask != null) {
      recoveryCompletionTask.cancel(true)
    }
    if (checkForWorkerTimeOutTask != null) {
      checkForWorkerTimeOutTask.cancel(true)
    }
    forwardMessageThread.shutdownNow()
    webUi.stop()
    restServer.foreach(_.stop())
    masterMetricsSystem.stop()
    applicationMetricsSystem.stop()
    persistenceEngine.close()
    leaderElectionAgent.stop()
  }

  /**
   * electedLeader方法只是向Master自身发送了ElectedLeader消息。
   * Master的receive方法中实现了对ElectedLeader消息的处理
   */
  override def electedLeader() {
    self.send(ElectedLeader)
  }

  override def revokedLeadership() {
    self.send(RevokedLeadership)
  }
//  接受信息判断主从情况
  override def receive: PartialFunction[Any, Unit] = {
    case ElectedLeader =>
//      从持久化引擎中读取出持久化的ApplicationInfo、DriverInfo、WorkerInfo等信息。
      val (storedApps, storedDrivers, storedWorkers) = persistenceEngine.readPersistedData(rpcEnv)

      /**
       * 如果没有任何持久化信息，那么将Master的当前状态设置为激活（ALIVE），否则将Master的当前状态设置为恢复中（RECOVERING）。
       */
      state = if (storedApps.isEmpty && storedDrivers.isEmpty && storedWorkers.isEmpty) {
        RecoveryState.ALIVE
      } else {
        RecoveryState.RECOVERING
      }
      logInfo("I have been elected leader! New state: " + state)
      if (state == RecoveryState.RECOVERING) {
//      如果Master的当前状态为RECOVERING，则调用beginRecovery方法对整个集群的状态进行恢复
        beginRecovery(storedApps, storedDrivers, storedWorkers)

        /**
         * 在集群状态恢复完成后，创建延时任务recoveryCompletionTask，在常量WORKER_TIMEOUT_MS指定的时间后向Master自身发送CompleteRecovery消息
         */
        recoveryCompletionTask = forwardMessageThread.schedule(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError {
            self.send(CompleteRecovery)
          }
        }, WORKER_TIMEOUT_MS, TimeUnit.MILLISECONDS)
      }
//    重新恢复主
    case CompleteRecovery => completeRecovery()
//    如果主被撤销，提示退出
    case RevokedLeadership =>
      logError("Leadership has been revoked -- master shutting down.")
      System.exit(0)
//  如果是注册worker
//  这里需要worker、master信息和worker配置信息
    case RegisterWorker(
      id, workerHost, workerPort, workerRef, cores, memory, workerWebUiUrl, masterAddress) =>
      logInfo("Registering worker %s:%d with %d cores, %s RAM".format(
        workerHost, workerPort, cores, Utils.megabytesToString(memory)))
      // 判断Master存活，进行Worker提示或者注册
      if (state == RecoveryState.STANDBY) { // 如果Master的状态是STANDBY，那么向Worker回复MasterInStandby消息。
        workerRef.send(MasterInStandby)
        // master存储，根据id判断worker是否已经存在
      } else if (idToWorker.contains(id)) { // 如果idToWorker中已经有了要注册的Worker的信息，那么说明Worker重复注册，
        // Master向Worker回复RegisterWorkerFailed消息。
        workerRef.send(RegisterWorkerFailed("Duplicate worker ID"))
      } else {
        // 利用RegisterWorker消息携带的信息创建WorkerInfo。
        val worker = new WorkerInfo(id, workerHost, workerPort, cores, memory,
          workerRef, workerWebUiUrl)
        if (registerWorker(worker)) {
          /**
           * 如果注册成功，那么对WorkerInfo进行持久化并向Worker回复RegisteredWorker消息，最后调用schedule方法进行资源的调度。
           * 如果注册失败，则向Worker回复RegisterWorkerFailed消息。
           */
          persistenceEngine.addWorker(worker)
          workerRef.send(RegisteredWorker(self, masterWebUiUrl, masterAddress))
          schedule()
        } else {
          val workerAddress = worker.endpoint.address
          logWarning("Worker registration failed. Attempted to re-register worker at same " +
            "address: " + workerAddress)
          workerRef.send(RegisterWorkerFailed("Attempted to re-register worker at same address: "
            + workerAddress))
        }
      }

    case RegisterApplication(description, driver) =>
      // TODO Prevent repeated registrations from some driver
      // 如果Master当前的状态是STANDBY，那么不作任何处理。
      if (state == RecoveryState.STANDBY) {
        // ignore, don't send response
      } else {
        logInfo("Registering app " + description.name)
        /**
         * 利用RegisterApplication消息携带的信息调用createApplication方法，创建ApplicationInfo。createApplication方法中会给
         * Application分配ID。此ID的生成规则为app-${yyyyMMddHHmmss}-${nextAppNumber}。
         */
        val app = createApplication(description, driver)
        // 注册应用
        registerApplication(app)
        logInfo("Registered app " + description.name + " with ID " + app.id)
        // 对ApplicationInfo持久化
        persistenceEngine.addApplication(app)

        /**
         *  向ClientEndpoint发送RegisteredApplication消息（此消息将携带为Application生成的ID和Master自身的RpcEndpointRef），
         *  以表示注册Application信息成功。
         */
        driver.send(RegisteredApplication(app.id, self))

        /**
         * 调用schedule方法进行资源调度。
         */
        schedule()
      }

    case ExecutorStateChanged(appId, execId, state, message, exitStatus) =>
      val execOption = idToApp.get(appId).flatMap(app => app.executors.get(execId))
      execOption match {
        case Some(exec) =>
          val appInfo = idToApp(appId)
          val oldState = exec.state
          exec.state = state

          if (state == ExecutorState.RUNNING) {
            assert(oldState == ExecutorState.LAUNCHING,
              s"executor $execId state transfer from $oldState to RUNNING is illegal")
            appInfo.resetRetryCount()
          }

          exec.application.driver.send(ExecutorUpdated(execId, state, message, exitStatus, false))

          if (ExecutorState.isFinished(state)) {
            // Remove this executor from the worker and app
            logInfo(s"Removing executor ${exec.fullId} because it is $state")
            // If an application has already finished, preserve its
            // state to display its information properly on the UI
            if (!appInfo.isFinished) {
              appInfo.removeExecutor(exec)
            }
            exec.worker.removeExecutor(exec)

            val normalExit = exitStatus == Some(0)
            // Only retry certain number of times so we don't go into an infinite loop.
            // Important note: this code path is not exercised by tests, so be very careful when
            // changing this `if` condition.
            if (!normalExit
                && appInfo.incrementRetryCount() >= MAX_EXECUTOR_RETRIES
                && MAX_EXECUTOR_RETRIES >= 0) { // < 0 disables this application-killing path
              val execs = appInfo.executors.values
              if (!execs.exists(_.state == ExecutorState.RUNNING)) {
                logError(s"Application ${appInfo.desc.name} with ID ${appInfo.id} failed " +
                  s"${appInfo.retryCount} times; removing it")
                removeApplication(appInfo, ApplicationState.FAILED)
              }
            }
          }
          schedule()
        case None =>
          logWarning(s"Got status update for unknown executor $appId/$execId")
      }

    case DriverStateChanged(driverId, state, exception) =>
      state match {
        case DriverState.ERROR | DriverState.FINISHED | DriverState.KILLED | DriverState.FAILED =>
          removeDriver(driverId, state, exception)
        case _ =>
          throw new Exception(s"Received unexpected state update for driver $driverId: $state")
      }

    case Heartbeat(workerId, worker) =>
      idToWorker.get(workerId) match {
        case Some(workerInfo) =>
          workerInfo.lastHeartbeat = System.currentTimeMillis()
        case None =>
          if (workers.map(_.id).contains(workerId)) {
            logWarning(s"Got heartbeat from unregistered worker $workerId." +
              " Asking it to re-register.")
            worker.send(ReconnectWorker(masterUrl))
          } else {
            logWarning(s"Got heartbeat from unregistered worker $workerId." +
              " This worker was never registered, so ignoring the heartbeat.")
          }
      }

    case MasterChangeAcknowledged(appId) =>
      idToApp.get(appId) match {
        case Some(app) =>
          logInfo("Application has been re-registered: " + appId)
          app.state = ApplicationState.WAITING
        case None =>
          logWarning("Master change ack from unknown app: " + appId)
      }

      if (canCompleteRecovery) { completeRecovery() }

    case WorkerSchedulerStateResponse(workerId, executors, driverIds) =>
      idToWorker.get(workerId) match {
        case Some(worker) =>
          logInfo("Worker has been re-registered: " + workerId)
          worker.state = WorkerState.ALIVE

          val validExecutors = executors.filter(exec => idToApp.get(exec.appId).isDefined)
          for (exec <- validExecutors) {
            val app = idToApp.get(exec.appId).get
            val execInfo = app.addExecutor(worker, exec.cores, Some(exec.execId))
            worker.addExecutor(execInfo)
            execInfo.copyState(exec)
          }

          for (driverId <- driverIds) {
            drivers.find(_.id == driverId).foreach { driver =>
              driver.worker = Some(worker)
              driver.state = DriverState.RUNNING
              worker.addDriver(driver)
            }
          }
        case None =>
          logWarning("Scheduler state from unknown worker: " + workerId)
      }

      if (canCompleteRecovery) { completeRecovery() }

    /**
     * Worker在向Master注册成功后，会向Master发送WorkerLatestState消息。Worker-LatestState消息将携带Worker的身份标识、Worker节点的
     * 所有Executor的描述信息、调度到当前Worker的所有Driver的身份标识。Master接收到WorkerLatestState消息的处理如下。
     */
    case WorkerLatestState(workerId, executors, driverIds) =>
      // 根据Worker的身份标识，从idToWorker取出注册的WorkerInfo。
      idToWorker.get(workerId) match {
        case Some(worker) =>
          // 遍历executors中的每个ExecutorDescription
          for (exec <- executors) {
            // 遍历executors中的每个ExecutorDescription，与WorkerInfo的executors中保存的ExecutorDesc按照应用ID和Executor ID
            // 进行匹配。对于匹配不成功的，向Worker发送KillExecutor消息以“杀死”Executor。
            val executorMatches = worker.executors.exists {
              case (_, e) => e.application.id == exec.appId && e.id == exec.execId
            }
            if (!executorMatches) {
              // master doesn't recognize this executor. So just tell worker to kill it.
              worker.endpoint.send(KillExecutor(masterUrl, exec.appId, exec.execId))
            }
          }

          for (driverId <- driverIds) {
            val driverMatches = worker.drivers.exists { case (id, _) => id == driverId }
            if (!driverMatches) {
              // master doesn't recognize this driver. So just tell worker to kill it.
              worker.endpoint.send(KillDriver(driverId))
            }
          }
        case None =>
          logWarning("Worker state from unknown worker: " + workerId)
      }

    case UnregisterApplication(applicationId) =>
      logInfo(s"Received unregister request from application $applicationId")
      idToApp.get(applicationId).foreach(finishApplication)
    // 心跳检测
    case CheckForWorkerTimeOut =>
      timeOutDeadWorkers()

  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RequestSubmitDriver(description) =>
      if (state != RecoveryState.ALIVE) {
        val msg = s"${Utils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          "Can only accept driver submissions in ALIVE state."
        context.reply(SubmitDriverResponse(self, false, None, msg))
      } else {
        logInfo("Driver submitted " + description.command.mainClass)
        val driver = createDriver(description)
        persistenceEngine.addDriver(driver)
        waitingDrivers += driver
        drivers.add(driver)
        schedule()

        // TODO: It might be good to instead have the submission client poll the master to determine
        //       the current status of the driver. For now it's simply "fire and forget".

        context.reply(SubmitDriverResponse(self, true, Some(driver.id),
          s"Driver successfully submitted as ${driver.id}"))
      }

    case RequestKillDriver(driverId) =>
      if (state != RecoveryState.ALIVE) {
        val msg = s"${Utils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          s"Can only kill drivers in ALIVE state."
        context.reply(KillDriverResponse(self, driverId, success = false, msg))
      } else {
        logInfo("Asked to kill driver " + driverId)
        val driver = drivers.find(_.id == driverId)
        driver match {
          case Some(d) =>
            if (waitingDrivers.contains(d)) {
              waitingDrivers -= d
              self.send(DriverStateChanged(driverId, DriverState.KILLED, None))
            } else {
              // We just notify the worker to kill the driver here. The final bookkeeping occurs
              // on the return path when the worker submits a state change back to the master
              // to notify it that the driver was successfully killed.
              d.worker.foreach { w =>
                w.endpoint.send(KillDriver(driverId))
              }
            }
            // TODO: It would be nice for this to be a synchronous response
            val msg = s"Kill request for $driverId submitted"
            logInfo(msg)
            context.reply(KillDriverResponse(self, driverId, success = true, msg))
          case None =>
            val msg = s"Driver $driverId has already finished or does not exist"
            logWarning(msg)
            context.reply(KillDriverResponse(self, driverId, success = false, msg))
        }
      }

    case RequestDriverStatus(driverId) =>
      if (state != RecoveryState.ALIVE) {
        val msg = s"${Utils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          "Can only request driver status in ALIVE state."
        context.reply(
          DriverStatusResponse(found = false, None, None, None, Some(new Exception(msg))))
      } else {
        (drivers ++ completedDrivers).find(_.id == driverId) match {
          case Some(driver) =>
            context.reply(DriverStatusResponse(found = true, Some(driver.state),
              driver.worker.map(_.id), driver.worker.map(_.hostPort), driver.exception))
          case None =>
            context.reply(DriverStatusResponse(found = false, None, None, None, None))
        }
      }

    case RequestMasterState =>
      context.reply(MasterStateResponse(
        address.host, address.port, restServerBoundPort,
        workers.toArray, apps.toArray, completedApps.toArray,
        drivers.toArray, completedDrivers.toArray, state))

    case BoundPortsRequest =>
      context.reply(BoundPortsResponse(address.port, webUi.boundPort, restServerBoundPort))

    case RequestExecutors(appId, requestedTotal) =>
      context.reply(handleRequestExecutors(appId, requestedTotal))

    case KillExecutors(appId, executorIds) =>
      val formattedExecutorIds = formatExecutorIds(executorIds)
      context.reply(handleKillExecutors(appId, formattedExecutorIds))
  }

  override def onDisconnected(address: RpcAddress): Unit = {
    // The disconnected client could've been either a worker or an app; remove whichever it was
    logInfo(s"$address got disassociated, removing it.")
    addressToWorker.get(address).foreach(removeWorker(_, s"${address} got disassociated"))
    addressToApp.get(address).foreach(finishApplication)
    if (state == RecoveryState.RECOVERING && canCompleteRecovery) { completeRecovery() }
  }

  private def canCompleteRecovery =
    workers.count(_.state == WorkerState.UNKNOWN) == 0 &&
      apps.count(_.state == ApplicationState.UNKNOWN) == 0

  private def beginRecovery(storedApps: Seq[ApplicationInfo], storedDrivers: Seq[DriverInfo],
      storedWorkers: Seq[WorkerInfo]) {
//    遍历每个storedApps,进行重新注册
    for (app <- storedApps) {
      logInfo("Trying to recover app: " + app.id)
      try {
        // 注册应用
        /**
         * 调用registerApplication方法（见代码清单9-44）将ApplicationInfo添加到
         * apps、idToApp、endpointToApp、addressToApp、waitingApps等缓存中。
         */
        registerApplication(app)
//        将应用的状态设置成UNKONWN
        app.state = ApplicationState.UNKNOWN
        /**
         * 向提交应用程序的Driver发送MasterChanged消息（此消息将携带被选举为领导的Master和此Master的masterWebUiUrl属性）。
         * Driver接收到MasterChanged消息后，将自身的master属性修改为当前Master的RpcEndpointRef，并将alreadyDisconnected
         * 设置为false，最后向Master发送MasterChangeAcknowledged消息
         */
        app.driver.send(MasterChanged(self, masterWebUiUrl))
      } catch {
        case e: Exception => logInfo("App " + app.id + " had exception on reconnect")
      }
    }

    for (driver <- storedDrivers) {
      // Here we just read in the list of drivers. Any drivers associated with now-lost workers
      // will be re-launched when we detect that the worker is missing.
      drivers += driver
    }

    for (worker <- storedWorkers) {
      logInfo("Trying to recover worker: " + worker.id)
      try {
        registerWorker(worker)
        worker.state = WorkerState.UNKNOWN
        worker.endpoint.send(MasterChanged(self, masterWebUiUrl))
      } catch {
        case e: Exception => logInfo("Worker " + worker.id + " had exception on reconnect")
      }
    }
  }

  private def completeRecovery() {
    // Ensure "only-once" recovery semantics using a short synchronization period.
    // 如果Master的状态不是RECOVERING，直接返回。
    if (state != RecoveryState.RECOVERING) { return }
    state = RecoveryState.COMPLETING_RECOVERY

    // Kill off any workers and apps that didn't respond to us.
    /**
     * 将workers中状态为UNKNOWN的所有WorkerInfo，通过调用removeWorker方法从Master中移除。
     */
    workers.filter(_.state == WorkerState.UNKNOWN).foreach(
      removeWorker(_, "Not responding for recovery"))
    /**
     * 将apps中状态为UNKNOWN的所有ApplicationInfo，通过调用finishApplication方法从Master中移除。
     */
    apps.filter(_.state == ApplicationState.UNKNOWN).foreach(finishApplication)

    // Update the state of recovered apps to RUNNING
    apps.filter(_.state == ApplicationState.WAITING).foreach(_.state = ApplicationState.RUNNING)

    // Reschedule drivers which were not claimed by any workers
    /**
     * 从drivers中过滤出还没有分配Worker的所有DriverInfo，如果Driver是被监管的，则调用relaunchDriver方法重新调度运行指定的Driver，
     * 否则调用removeDriver方法移除Master维护的关于指定Driver的相关信息和状态。
     */
    drivers.filter(_.worker.isEmpty).foreach { d =>
      logWarning(s"Driver ${d.id} was not found after master recovery")
      if (d.desc.supervise) {
        logWarning(s"Re-launching ${d.id}")
        relaunchDriver(d) // 重新调度运行指定的Driver
      } else {
        removeDriver(d.id, DriverState.ERROR, None)
        logWarning(s"Did not re-launch ${d.id} because it was not supervised")
      }
    }

    state = RecoveryState.ALIVE
    schedule()
    logInfo("Recovery complete - resuming operations!")
  }

  /**
   * Schedule executors to be launched on the workers.
   * Returns an array containing number of cores assigned to each worker.
   *
   * There are two modes of launching executors. The first attempts to spread out an application's
   * executors on as many workers as possible, while the second does the opposite (i.e. launch them
   * on as few workers as possible). The former is usually better for data locality purposes and is
   * the default.
   *
   * The number of cores assigned to each executor is configurable. When this is explicitly set,
   * multiple executors from the same application may be launched on the same worker if the worker
   * has enough cores and memory. Otherwise, each executor grabs all the cores available on the
   * worker by default, in which case only one executor per application may be launched on each
   * worker during one single schedule iteration.
   * Note that when `spark.executor.cores` is not set, we may still launch multiple executors from
   * the same application on the same worker. Consider appA and appB both have one executor running
   * on worker1, and appA.coresLeft > 0, then appB is finished and release all its cores on worker1,
   * thus for the next schedule iteration, appA launches a new executor that grabs all the free
   * cores on worker1, therefore we get multiple executors from appA running on worker1.
   *
   * It is important to allocate coresPerExecutor on each worker at a time (instead of 1 core
   * at a time). Consider the following example: cluster has 4 workers with 16 cores each.
   * User requests 3 executors (spark.cores.max = 48, spark.executor.cores = 16). If 1 core is
   * allocated at a time, 12 cores from each worker would be assigned to each executor.
   * Since 12 < 16, no executors would launch [SPARK-8881].
   */
  private def scheduleExecutorsOnWorkers(
      app: ApplicationInfo,
      usableWorkers: Array[WorkerInfo], // 缓存从workers中选出的状态为ALIVE、空闲空间满足ApplicationInfo要求的每个Executor使用的内存大小、空闲内核数满足coresPerExecutor的所有WorkerInfo。
      spreadOutApps: Boolean): Array[Int] = {
    // Application要求的每个Executor所需的内核数。
    val coresPerExecutor = app.desc.coresPerExecutor
    // Application要求的每个Executor所需的最小内核数。
    val minCoresPerExecutor = coresPerExecutor.getOrElse(1)
    // 是否在每个Worker上只分配一个Executor。当Application没有配置coresPerExecutor时，oneExecutorPerWorker为true。
    val oneExecutorPerWorker = coresPerExecutor.isEmpty
    // Application要求的每个Executor所需的内存大小（单位为字节）。
    val memoryPerExecutor = app.desc.memoryPerExecutorMB
    // 缓存从workers中选出的状态为ALIVE、空闲空间满足ApplicationInfo要求的每个Executor使用的内存大小、空闲内核数满足coresPerExecutor的所有WorkerInfo。
    val numUsable = usableWorkers.length
    // 用于保存每个Worker给Application分配的内核数的数组。通过数组索引与usableWorkers中的WorkerInfo相对应。
    val assignedCores = new Array[Int](numUsable) // Number of cores to give to each worker
    // 用于保存每个Worker给应用分配的Executor数的数组。通过数组索引与usableWorkers中的WorkerInfo相对应。
    val assignedExecutors = new Array[Int](numUsable) // Number of new executors on each worker
    // 给Application要分配的内核数。coresToAssign取usableWorkers中所有WorkerInfo的空闲内核数之和与Application还需的内核数中的最小值。
    /**
     * app.coresLeft：Application还需的内核数中的最小值
     * usableWorkers.map(_.coresFree).sum)： 表示WorkerInfo的空闲内核数之和
     */
    var coresToAssign = math.min(app.coresLeft, usableWorkers.map(_.coresFree).sum)

    /** Return whether the specified worker can launch an executor for this app. */
    /**
     * 判断usableWorkers中索引为参数pos指定的位置上的WorkerInfo是否能运行Executor
     *
     * @param pos
     * @return
     */
    def canLaunchExecutor(pos: Int): Boolean = {
      // Executor可用于被分配的CPU核心数大于Application需要的每个Executor的最小核心数
      val keepScheduling = coresToAssign >= minCoresPerExecutor

      // usableWorkers中索引位置为pos的WorkerInfo的空闲的cores - 已经被Application分配的内核数量 > Application需要的每个Executor的最小核心数
      val enoughCores = usableWorkers(pos).coresFree - assignedCores(pos) >= minCoresPerExecutor

      // If we allow multiple executors per worker, then we can always launch new executors.
      // Otherwise, if there is already an executor on this worker, just give it more cores.
      val launchingNewExecutor = !oneExecutorPerWorker || assignedExecutors(pos) == 0
      if (launchingNewExecutor) {
        val assignedMemory = assignedExecutors(pos) * memoryPerExecutor
        val enoughMemory = usableWorkers(pos).memoryFree - assignedMemory >= memoryPerExecutor
        val underLimit = assignedExecutors.sum + app.executors.size < app.executorLimit
        keepScheduling && enoughCores && enoughMemory && underLimit
      } else {
        // We're adding cores to an existing executor, so no need
        // to check memory and executor limits
        keepScheduling && enoughCores
      }
    }

    // Keep launching executors until no more workers can accommodate any
    // more executors, or if we have reached this application's limits
    var freeWorkers = (0 until numUsable).filter(canLaunchExecutor)
    while (freeWorkers.nonEmpty) {
      freeWorkers.foreach { pos =>
        var keepScheduling = true
        while (keepScheduling && canLaunchExecutor(pos)) {
          // 由于给Application分配Executor时，每个Executor都至少需要minCoresPerExecutor指定大小的内核，
          // 因此将coresToAssign减去minCoresPerExecutor的大小。
          coresToAssign -= minCoresPerExecutor
          //将usableWorkers的pos位置的WorkerInfo已经分配的内核数（即assignedCores的pos位置的数字）增加minCoresPerExecutor的大小。
          assignedCores(pos) += minCoresPerExecutor

          // If we are launching one executor per worker, then every iteration assigns 1 core
          // to the executor. Otherwise, every iteration assigns cores to a new executor.
          // 如果oneExecutorPerWorker为true，则将assignedExecutors的索引为pos的值设置为1，否则将assignedExecutors的索引为pos的值增1。
          if (oneExecutorPerWorker) {
            assignedExecutors(pos) = 1
          } else {
            assignedExecutors(pos) += 1
          }

          // Spreading out an application means spreading out its executors across as
          // many workers as possible. If we are not spreading out, then we should keep
          // scheduling executors on this worker until we use all of its resources.
          // Otherwise, just move on to the next worker.
          if (spreadOutApps) {
            keepScheduling = false
          }
        }
      }
      freeWorkers = freeWorkers.filter(canLaunchExecutor)
    }
    assignedCores
  }

  /**
   * Schedule and launch executors on workers
   */
  private def startExecutorsOnWorkers(): Unit = {
    // Right now this is a very simple FIFO scheduler. We keep trying to fit in the first app
    // in the queue, then the second app, etc.
    for (app <- waitingApps) {
      val coresPerExecutor = app.desc.coresPerExecutor.getOrElse(1) // 获取Application要求的每个Executor使用的内核数coresPerExecutor。
      // If the cores left is less than the coresPerExecutor,the cores left will not be allocated
      if (app.coresLeft >= coresPerExecutor) {
        // Filter out workers that don't have enough resources to launch an executor
        /**
         * 找出workers中状态为ALIVE、空闲空间满足Application要求的每个Executor使用的内存大小、空闲内核数满足coresPerExecutor的所有
         * WorkerInfo，并对这些WorkerInfo按照空闲内核数倒序排列，这样可以优先将应用分配给内核资源充足的Worker。
         */
        val usableWorkers = workers.toArray.filter(_.state == WorkerState.ALIVE)
          .filter(worker => worker.memoryFree >= app.desc.memoryPerExecutorMB &&
            worker.coresFree >= coresPerExecutor)
          .sortBy(_.coresFree).reverse
        /**
         * scheduleExecutorsOnWorkers方法返回在各个Worker上分配的内核数。
         */
        val assignedCores = scheduleExecutorsOnWorkers(app, usableWorkers, spreadOutApps)

        // Now that we've decided how many cores to allocate on each worker, let's allocate them
        for (pos <- 0 until usableWorkers.length if assignedCores(pos) > 0) {
          // 将Worker上的资源分配给Executor。
          allocateWorkerResourceToExecutors(
            app, assignedCores(pos), app.desc.coresPerExecutor, usableWorkers(pos))
        }
      }
    }
  }

  /**
   * Allocate a worker's resources to one or more executors.
   * @param app the info of the application which the executors belong to
   * @param assignedCores number of cores on this worker for this application
   * @param coresPerExecutor number of cores per executor
   * @param worker the worker info
   */
  private def allocateWorkerResourceToExecutors(
      app: ApplicationInfo,
      assignedCores: Int,
      coresPerExecutor: Option[Int],
      worker: WorkerInfo): Unit = {
    // If the number of cores per executor is specified, we divide the cores assigned
    // to this worker evenly among the executors with no remainder.
    // Otherwise, we launch a single executor that grabs all the assignedCores on this worker.
    /**
     * 根据Worker分配给Application的内核数（assignedCores）与每个Executor需要的内核数（coresPerExecutor），
     * 计算在Worker上要运行的Executor数量（numExecutors）。如果没有指定coresPerExecutor，说明assignedCores
     * 指定的所有内核都由一个Executor使用。
     */
    val numExecutors = coresPerExecutor.map { assignedCores / _ }.getOrElse(1)
    val coresToAssign = coresPerExecutor.getOrElse(assignedCores)

    /**
     * 按照numExecutors的值，多次调用launchExecutor方法，在Worker上创建、运行Executor，并将ApplicationInfo的状态设置为RUNNING。
     */
    for (i <- 1 to numExecutors) {
      val exec = app.addExecutor(worker, coresToAssign)
      launchExecutor(worker, exec)
      app.state = ApplicationState.RUNNING
    }
  }

  /**
   * Schedule the currently available resources among waiting apps. This method will be called
   * every time a new app joins or resource availability changes.
   */
  private def schedule(): Unit = {
    // 如果Master的状态不是ALIVE，那么直接返回。
    if (state != RecoveryState.ALIVE) { //都已经运行了，因此不用再重新分配资源
      return
    }
    // Drivers take strict precedence over executors
    /**
     * 过滤出workers中缓存的状态为ALIVE的WorkerInfo，并随机洗牌，以避免Driver总是分配给小部分的Worker。
     */
    val shuffledAliveWorkers = Random.shuffle(workers.toSeq.filter(_.state == WorkerState.ALIVE))
    val numWorkersAlive = shuffledAliveWorkers.size
    var curPos = 0
    /**
     * 遍历waitingDrivers中的DriverInfo
     */
    for (driver <- waitingDrivers.toList) { // iterate over a copy of waitingDrivers
      // We assign workers to each waiting driver in a round-robin fashion. For each driver, we
      // start from the last worker that was assigned a driver, and continue onwards until we have
      // explored all alive workers.
      var launched = false
      var numWorkersVisited = 0

      /**
       * 步筛选的WorkerInfo中，挑出内存大小和内核数都满足Driver需要的。
       */
      while (numWorkersVisited < numWorkersAlive && !launched) {
        val worker = shuffledAliveWorkers(curPos)
        numWorkersVisited += 1
        if (worker.memoryFree >= driver.desc.mem && worker.coresFree >= driver.desc.cores) {
          // 调用launchDriver运行Driver
          launchDriver(worker, driver)
          /**
           * 将DriverInfo从waitingDrivers中移除。
           */
          waitingDrivers -= driver
          launched = true
        }
        curPos = (curPos + 1) % numWorkersAlive
      }
    }

    /**
     * 在Worker上启动Executor
     */
    startExecutorsOnWorkers()
  }

  private def launchExecutor(worker: WorkerInfo, exec: ExecutorDesc): Unit = {
    logInfo("Launching executor " + exec.fullId + " on worker " + worker.id)
    // 调用WorkerInfo的addExecutor方法添加新的Executor的信息
    worker.addExecutor(exec)
    // 向Worker发送LaunchExecutor消息（此消息携带着masterUrl、Application的ID、Executor的ID、Application的描述信息
    // ApplicationDescription、Executor分配获得的内核数、Executor分配获得的内存大小等信息）
    worker.endpoint.send(LaunchExecutor(masterUrl,
      exec.application.id, exec.id, exec.application.desc, exec.cores, exec.memory))
    // 向提交应用的Driver发送ExecutorAdded消息（此消息携带着Executor的ID、Worker的ID、Worker的host和port、Executor分配获得的内核数、
    // Executor分配获得的内存大小等信息）。ClientEndpoint接收到ExecutorAdded消息后，将调用StandaloneAppClientListener的executorAdded方法。
    // 由于StandaloneAppClientListener只有StandaloneSchedulerBackend这一个实现类，因此实际上调用了StandaloneSchedulerBackend的executorAdded方法
    exec.application.driver.send(
      ExecutorAdded(exec.id, worker.id, worker.hostPort, exec.cores, exec.memory))
  }

  private def registerWorker(worker: WorkerInfo): Boolean = {
    // There may be one or more refs to dead workers on this same node (w/ different ID's),
    // remove them.
    // 从workers中移除host和port与要注册的WorkerInfo的host和port一样，且状态为DEAD的WorkerInfo。
    workers.filter { w =>
      (w.host == worker.host && w.port == worker.port) && (w.state == WorkerState.DEAD)
    }.foreach { w =>
      workers -= w
    }

    val workerAddress = worker.endpoint.address

    /**
     * 如果addressToWorker中包含地址相同的WorkerInfo，并且此WorkerInfo的状态为UNKNOWN，那么调用removeWorker方法，
     * 移除此WorkerInfo的相关状态，否则返回false。
     */
    if (addressToWorker.contains(workerAddress)) {
      val oldWorker = addressToWorker(workerAddress)
      if (oldWorker.state == WorkerState.UNKNOWN) {
        // A worker registering from UNKNOWN implies that the worker was restarted during recovery.
        // The old worker must thus be dead, so we will remove it and accept the new worker.
        removeWorker(oldWorker, "Worker replaced by a new worker with same address")
      } else {
        logInfo("Attempted to re-register worker at same address: " + workerAddress)
        return false
      }
    }

    /**
     * 如果addressToWorker中包含地址相同的WorkerInfo，并且此WorkerInfo的状态为UNKNOWN，那么调用removeWorker方法，
     * 移除此WorkerInfo的相关状态，否则返回false。
     */
    workers += worker
    idToWorker(worker.id) = worker
    addressToWorker(workerAddress) = worker
    true
  }

  private def removeWorker(worker: WorkerInfo, msg: String) {
    logInfo("Removing worker " + worker.id + " on " + worker.host + ":" + worker.port)
    worker.setState(WorkerState.DEAD)
    idToWorker -= worker.id
    addressToWorker -= worker.endpoint.address

    for (exec <- worker.executors.values) {
      logInfo("Telling app of lost executor: " + exec.id)
      exec.application.driver.send(ExecutorUpdated(
        exec.id, ExecutorState.LOST, Some("worker lost"), None, workerLost = true))
      exec.state = ExecutorState.LOST
      exec.application.removeExecutor(exec)
    }
    for (driver <- worker.drivers.values) {
      if (driver.desc.supervise) {
        logInfo(s"Re-launching ${driver.id}")
        relaunchDriver(driver)
      } else {
        logInfo(s"Not re-launching ${driver.id} because it was not supervised")
        removeDriver(driver.id, DriverState.ERROR, None)
      }
    }
    logInfo(s"Telling app of lost worker: " + worker.id)
    apps.filterNot(completedApps.contains(_)).foreach { app =>
      app.driver.send(WorkerRemoved(worker.id, worker.host, msg))
    }
    persistenceEngine.removeWorker(worker)
  }

  private def relaunchDriver(driver: DriverInfo) {
    // We must setup a new driver with a new driver id here, because the original driver may
    // be still running. Consider this scenario: a worker is network partitioned with master,
    // the master then relaunches driver driverID1 with a driver id driverID2, then the worker
    // reconnects to master. From this point on, if driverID2 is equal to driverID1, then master
    // can not distinguish the statusUpdate of the original driver and the newly relaunched one,
    // for example, when DriverStateChanged(driverID1, KILLED) arrives at master, master will
    // remove driverID1, so the newly relaunched driver disappears too. See SPARK-19900 for details.
    removeDriver(driver.id, DriverState.RELAUNCHING, None)
    val newDriver = createDriver(driver.desc)
    persistenceEngine.addDriver(newDriver)
    drivers.add(newDriver)
    waitingDrivers += newDriver

    schedule()
  }

  private def createApplication(desc: ApplicationDescription, driver: RpcEndpointRef):
      ApplicationInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    val appId = newApplicationId(date)
    new ApplicationInfo(now, appId, desc, date, driver, defaultCores)
  }

  private def registerApplication(app: ApplicationInfo): Unit = {
    // driver 地址
    val appAddress = app.driver.address
    if (addressToApp.contains(appAddress)) {
      logInfo("Attempted to re-register application at same address: " + appAddress)
      return
    }

    applicationMetricsSystem.registerSource(app.appSource)
//    保存app信息
    apps += app
//    保存appId和app的映射关系
    idToApp(app.id) = app
//    保存drvier与app的映射关系
    endpointToApp(app.driver) = app
//    rpc中注册的ip地址与app的映射关系
    addressToApp(appAddress) = app
//    保存队列中保存的应用
    waitingApps += app
  }

  private def finishApplication(app: ApplicationInfo) {
    removeApplication(app, ApplicationState.FINISHED)
  }

  def removeApplication(app: ApplicationInfo, state: ApplicationState.Value) {
    if (apps.contains(app)) {
      logInfo("Removing app " + app.id)
      apps -= app
      idToApp -= app.id
      endpointToApp -= app.driver
      addressToApp -= app.driver.address

      if (completedApps.size >= RETAINED_APPLICATIONS) {
        val toRemove = math.max(RETAINED_APPLICATIONS / 10, 1)
        completedApps.take(toRemove).foreach { a =>
          applicationMetricsSystem.removeSource(a.appSource)
        }
        completedApps.trimStart(toRemove)
      }
      completedApps += app // Remember it in our history
      waitingApps -= app

      for (exec <- app.executors.values) {
        killExecutor(exec)
      }
      app.markFinished(state)
      if (state != ApplicationState.FINISHED) {
        app.driver.send(ApplicationRemoved(state.toString))
      }
      persistenceEngine.removeApplication(app)
      schedule()

      // Tell all workers that the application has finished, so they can clean up any app state.
      workers.foreach { w =>
        w.endpoint.send(ApplicationFinished(app.id))
      }
    }
  }

  /**
   * Handle a request to set the target number of executors for this application.
   *
   * If the executor limit is adjusted upwards, new executors will be launched provided
   * that there are workers with sufficient resources. If it is adjusted downwards, however,
   * we do not kill existing executors until we explicitly receive a kill request.
   *
   * @return whether the application has previously registered with this Master.
   */
  private def handleRequestExecutors(appId: String, requestedTotal: Int): Boolean = {
    idToApp.get(appId) match {
      case Some(appInfo) =>
        logInfo(s"Application $appId requested to set total executors to $requestedTotal.")
        appInfo.executorLimit = requestedTotal
        schedule()
        true
      case None =>
        logWarning(s"Unknown application $appId requested $requestedTotal total executors.")
        false
    }
  }

  /**
   * Handle a kill request from the given application.
   *
   * This method assumes the executor limit has already been adjusted downwards through
   * a separate [[RequestExecutors]] message, such that we do not launch new executors
   * immediately after the old ones are removed.
   *
   * @return whether the application has previously registered with this Master.
   */
  private def handleKillExecutors(appId: String, executorIds: Seq[Int]): Boolean = {
    idToApp.get(appId) match {
      case Some(appInfo) =>
        logInfo(s"Application $appId requests to kill executors: " + executorIds.mkString(", "))
        val (known, unknown) = executorIds.partition(appInfo.executors.contains)
        known.foreach { executorId =>
          val desc = appInfo.executors(executorId)
          appInfo.removeExecutor(desc)
          killExecutor(desc)
        }
        if (unknown.nonEmpty) {
          logWarning(s"Application $appId attempted to kill non-existent executors: "
            + unknown.mkString(", "))
        }
        schedule()
        true
      case None =>
        logWarning(s"Unregistered application $appId requested us to kill executors!")
        false
    }
  }

  /**
   * Cast the given executor IDs to integers and filter out the ones that fail.
   *
   * All executors IDs should be integers since we launched these executors. However,
   * the kill interface on the driver side accepts arbitrary strings, so we need to
   * handle non-integer executor IDs just to be safe.
   */
  private def formatExecutorIds(executorIds: Seq[String]): Seq[Int] = {
    executorIds.flatMap { executorId =>
      try {
        Some(executorId.toInt)
      } catch {
        case e: NumberFormatException =>
          logError(s"Encountered executor with a non-integer ID: $executorId. Ignoring")
          None
      }
    }
  }

  /**
   * Ask the worker on which the specified executor is launched to kill the executor.
   */
  private def killExecutor(exec: ExecutorDesc): Unit = {
    exec.worker.removeExecutor(exec)
    exec.worker.endpoint.send(KillExecutor(masterUrl, exec.application.id, exec.id))
    exec.state = ExecutorState.KILLED
  }

  /** Generate a new app ID given an app's submission date */
  private def newApplicationId(submitDate: Date): String = {
    val appId = "app-%s-%04d".format(createDateFormat.format(submitDate), nextAppNumber)
    nextAppNumber += 1
    appId
  }

  /** Check for, and remove, any timed-out workers */
  private def timeOutDeadWorkers() {
    // Copy the workers into an array so we don't modify the hashset while iterating through it
    val currentTime = System.currentTimeMillis()

    // 过滤出所有超时的Worker，即当前时间与WORKER_TIMEOUT_MS之差仍然大于WorkerInfo的lastHeartbeat的Worker节点。
    val toRemove = workers.filter(_.lastHeartbeat < currentTime - WORKER_TIMEOUT_MS).toArray
    for (worker <- toRemove) {
      //如果WorkerInfo的状态不是DEAD，则调用removeWorker方法
      if (worker.state != WorkerState.DEAD) {
        logWarning("Removing %s because we got no heartbeat in %d seconds".format(
          worker.id, WORKER_TIMEOUT_MS / 1000))
        removeWorker(worker, s"Not receiving heartbeat for ${WORKER_TIMEOUT_MS / 1000} seconds")
      } else {
        // 如果WorkerInfo的状态是DEAD，则等待足够长的时间后将它从workers列表中移除。足够长的时间的计算公式为：REAPER_ITERATIONS与1的和再乘以WORKER_TIMEOUT_MS。
        if (worker.lastHeartbeat < currentTime - ((REAPER_ITERATIONS + 1) * WORKER_TIMEOUT_MS)) {
          workers -= worker // we've seen this DEAD worker in the UI, etc. for long enough; cull it
        }
      }
    }
  }

  private def newDriverId(submitDate: Date): String = {
    val appId = "driver-%s-%04d".format(createDateFormat.format(submitDate), nextDriverNumber)
    nextDriverNumber += 1
    appId
  }

  private def createDriver(desc: DriverDescription): DriverInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    new DriverInfo(now, newDriverId(date), desc, date)
  }

  /**
   * Master的launchDriver方法用于运行Driver
   *
   * @param worker
   * @param driver
   */
  private def launchDriver(worker: WorkerInfo, driver: DriverInfo) {
    logInfo("Launching driver " + driver.id + " on worker " + worker.id)
    // 在WorkerInfo和DriverInfo之间建立关系
    worker.addDriver(driver)
    driver.worker = Some(worker)
    // 向Worker发送LaunchDriver消息，Worker接收到LaunchDriver消息后将运行Driver
    worker.endpoint.send(LaunchDriver(driver.id, driver.desc))
    driver.state = DriverState.RUNNING
  }

  private def removeDriver(
      driverId: String,
      finalState: DriverState,
      exception: Option[Exception]) {
    drivers.find(d => d.id == driverId) match {
      case Some(driver) =>
        logInfo(s"Removing driver: $driverId")
        drivers -= driver
        if (completedDrivers.size >= RETAINED_DRIVERS) {
          val toRemove = math.max(RETAINED_DRIVERS / 10, 1)
          completedDrivers.trimStart(toRemove)
        }
        completedDrivers += driver
        persistenceEngine.removeDriver(driver)
        driver.state = finalState
        driver.exception = exception
        driver.worker.foreach(w => w.removeDriver(driver))
        schedule()
      case None =>
        logWarning(s"Asked to remove unknown driver: $driverId")
    }
  }
}

private[deploy] object Master extends Logging {
  val SYSTEM_NAME = "sparkMaster"
  val ENDPOINT_NAME = "Master"

  // spark-class传入参数：--port 7077 --webui-port 8080
  def main(argStrings: Array[String]) {
    Thread.setDefaultUncaughtExceptionHandler(new SparkUncaughtExceptionHandler(
      exitOnUncaughtException = false))
    // 初始化日志输出
    Utils.initDaemon(log)
    // 创建conf对象其实是读取Spark默认配置的参数
    // 创建MasterArguments以对执行main函数时传递的参数进行解析。在解析的过程中会将Spark属性配置文件中以spark．开头的属性保存到SparkConf。
    val conf = new SparkConf
    // 解析配置,初始化RPC服务和终端
    // 通过MasterArguments解析RPC需要的参数:host,port,webui-port
    val args = new MasterArguments(argStrings, conf)
    val (rpcEnv, _, _) = startRpcEnvAndEndpoint(args.host, args.port, args.webUiPort, conf)
//    执行到rpcEnv.awaitTermination()会处于等待Worker连接状态
    rpcEnv.awaitTermination()
  }

  /**
   * Start the Master and return a three tuple of:
   *   (1) The Master RpcEnv
   *   (2) The web UI bound port
   *   (3) The REST server bound port, if any
   */
  def startRpcEnvAndEndpoint(
      host: String,
      port: Int,
      webUiPort: Int,
      conf: SparkConf): (RpcEnv, Int, Option[Int]) = {
    // 初始化securityManager
    val securityMgr = new SecurityManager(conf)
    // 初始化RPC
    val rpcEnv = RpcEnv.create(SYSTEM_NAME, host, port, conf, securityMgr)
    // 向RPC注册master终端
    // 这里new Master的时候进行了Master的实例化
    // 创建Master，并且将Master（Master也继承了ThreadSafeRpcEndpoint）注册到刚创建的RpcEnv中，并获得Master的RpcEndpointRef。
    val masterEndpoint = rpcEnv.setupEndpoint(ENDPOINT_NAME,
      new Master(rpcEnv, rpcEnv.address, webUiPort, securityMgr, conf))
    // rest的绑定端口
    val portsResponse = masterEndpoint.askSync[BoundPortsResponse](BoundPortsRequest)
    (rpcEnv, portsResponse.webUIPort, portsResponse.restPort)
  }
}
