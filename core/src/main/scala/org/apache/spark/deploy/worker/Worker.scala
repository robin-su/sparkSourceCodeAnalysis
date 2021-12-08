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

package org.apache.spark.deploy.worker

import java.io.File
import java.io.IOException
import java.text.SimpleDateFormat
import java.util.{Date, Locale, UUID}
import java.util.concurrent._
import java.util.concurrent.{Future => JFuture, ScheduledFuture => JScheduledFuture}
import java.util.function.Supplier

import scala.collection.mutable.{HashMap, HashSet, LinkedHashMap}
import scala.concurrent.ExecutionContext
import scala.util.Random
import scala.util.control.NonFatal

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.{Command, ExecutorDescription, ExecutorState}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.ExternalShuffleService
import org.apache.spark.deploy.master.{DriverState, Master}
import org.apache.spark.deploy.worker.ui.WorkerWebUI
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.rpc._
import org.apache.spark.util.{SparkUncaughtExceptionHandler, ThreadUtils, Utils}

/**
 * Worker是Spark在local-cluster部署模式和Standalone部署模式中对工作节点的资源和Executor进行管理的服务。Worker一方面向Master汇报自
 * 身所管理的资源信息，一方面接收Master的命令运行Driver或者为Application运行Executor。同一个机器上可以同时部署多个Worker服务，一个
 * Worker也可以启动多个Executor。当Executor完成后，Worker将回收Executor使用的资源。
 *
 * @param rpcEnv 即RpcEnv
 * @param webUiPort 参数指定的WebUI的端口。
 * @param cores 内核数
 * @param memory 内存大小。
 * @param masterRpcAddresses Master的RpcEnv地址（即RpcAddress）的数组。由于一个集群为了可靠性和容错，需要多个Master节点，因此用数组来存储它们的RpcEnv地址。
 * @param endpointName Worker注册到RpcEnv的名称。
 * @param workDirPath  字符串表示的Worker的工作目录。
 * @param conf 即SparkConf。
 * @param securityMgr 即SecurityManager。
 * @param externalShuffleServiceSupplier
 */
private[deploy] class Worker(
    // 定义worker需要的参数
    override val rpcEnv: RpcEnv,
    webUiPort: Int,
    cores: Int,
    memory: Int,
    masterRpcAddresses: Array[RpcAddress],
    endpointName: String,
    workDirPath: String = null,
    val conf: SparkConf,
    val securityMgr: SecurityManager,
    externalShuffleServiceSupplier: Supplier[ExternalShuffleService] = null)
  extends ThreadSafeRpcEndpoint with Logging {

//  获取rpc的host和port
  private val host = rpcEnv.address.host
  private val port = rpcEnv.address.port

  Utils.checkHost(host)
  assert (port > 0)

  // A scheduled executor used to send messages at the specified time.
//  用于发送消息的调度执行器（ScheduledThreadPoolExecutor）。forwordMessageScheduler只能调度执行一个线程，执行的线程以worker-forward-message-scheduler作为名称。
  private val forwordMessageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-forward-message-scheduler")

  // A separated thread to clean up the workDir and the directories of finished applications.
  // Used to provide the implicit parameter of `Future` methods.
  // 通过Executors的newSingleThreadExecutor方法创建的线程执行器，用于清理Worker的工作目录。由cleanupThreadExecutor执行的线程以worker-cleanup-thread作为名称
  private val cleanupThreadExecutor = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonSingleThreadExecutor("worker-cleanup-thread"))

  // For worker and executor IDs
  // 获取时间作为worker及executor的ID
  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US)
  // Send a heartbeat every (heartbeat timeout) / 4 milliseconds
//  向Master发送心跳的时间间隔，是spark.worker.timeout属性值的1/4，默认为15秒。
  private val HEARTBEAT_MILLIS = conf.getLong("spark.worker.timeout", 60) * 1000 / 4

  // Model retries to connect to the master, after Hadoop's model.
  // The first six attempts to reconnect are in shorter intervals (between 5 and 15 seconds)
  // Afterwards, the next 10 attempts are between 30 and 90 seconds.
  // A bit of randomness is introduced so that not all of the workers attempt to reconnect at
  // the same time.
//  此常量固定为6，代表连接Master的前六次尝试。
  private val INITIAL_REGISTRATION_RETRIES = 6
  // 此常量固定为16，代表连接Master最多有16次尝试。
  private val TOTAL_REGISTRATION_RETRIES = INITIAL_REGISTRATION_RETRIES + 10
  // 此常量固定为0.500，是为了确保尝试的时间间隔不至于过小的下边界。
  private val FUZZ_MULTIPLIER_INTERVAL_LOWER_BOUND = 0.500
  /**
   * :0～1范围的随机数与FUZZ_MULTIP-LIER_INTERVAL_LOWER_BOUND的和，所以REGISTRATION_RETRY_FUZZ_MU-LTIPLIER的真实范围在0.5～1.5
   *  之间。加入随机数是为了避免各个Worker在同一时间发送心跳。
   */
  private val REGISTRATION_RETRY_FUZZ_MULTIPLIER = {
    val randomNumberGenerator = new Random(UUID.randomUUID.getMostSignificantBits)
    // 每次重连间隔+0.5秒 + 随机数
    randomNumberGenerator.nextDouble + FUZZ_MULTIPLIER_INTERVAL_LOWER_BOUND
  }
  // 代表前六次尝试的时间间隔。INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS取10和REGIST-RATION_RETRY_FUZZ_MULTIPLIER
  // 乘积的最近整数，因此这六次尝试的时间间隔在5～15s之间。
  private val INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS = (math.round(10 *
    REGISTRATION_RETRY_FUZZ_MULTIPLIER))
//   代表最后十次尝试的时间间隔。PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS取60和REGISTRATION_RETRY_FUZZ_MULTIPLIER乘积的
//   最近整数，因此最后十次尝试的时间间隔在30～90s之间。
  private val PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS = (math.round(60
    * REGISTRATION_RETRY_FUZZ_MULTIPLIER))
  // 是否对旧的Application产生的文件夹及文件进行清理。可通过spark.worker.cleanup.enabled属性配置，默认为false。
  private val CLEANUP_ENABLED = conf.getBoolean("spark.worker.cleanup.enabled", false)
  // How often worker will clean up old app folders
  // 对旧的Application产生的文件夹及文件进行清理的时间间隔。可通过spark.worker.cleanup.interval属性配置，默认为1800秒。
  private val CLEANUP_INTERVAL_MILLIS =
    conf.getLong("spark.worker.cleanup.interval", 60 * 30) * 1000
  // TTL for app folders/data;  after TTL expires it will be cleaned up
  // 程序和数据保存时间,到期才会进行清理,默认永久保存
  private val APP_DATA_RETENTION_SECONDS =
    conf.getLong("spark.worker.cleanup.appDataTtl", 7 * 24 * 3600)

  // Whether or not cleanup the non-shuffle files on executor exits.
  private val CLEANUP_NON_SHUFFLE_FILES_ENABLED =
    conf.getBoolean("spark.storage.cleanupFilesAfterExecutorExit", true)
  // 是否开启测试
  private val testing: Boolean = sys.props.contains("spark.testing")
  /**
   * Master的RpcEndpointRef。
   */
  private var master: Option[RpcEndpointRef] = None

  /**
   * Whether to use the master address in `masterRpcAddresses` if possible. If it's disabled, Worker
   * will just use the address received from Master.
   */
  // 是否使用RPC中的master的地址,否则使用从mastr获取的地址,也就是host:port,默认关闭
  private val preferConfiguredMasterAddress =
    conf.getBoolean("spark.worker.preferConfiguredMasterAddress", false)
  /**
   * The master address to connect in case of failure. When the connection is broken, worker will
   * use this address to connect. This is usually just one of `masterRpcAddresses`. However, when
   * a master is restarted or takes over leadership, it will be an address sent from master, which
   * may not be in `masterRpcAddresses`.
   */
  // 断开后重连的master地址
  // 一般是重连RPC中注册的master地址,但master重启或选主后的新的地址可能不在RPC中
  private var masterAddressToConnect: Option[RpcAddress] = None
  /**
   * 处于激活（ALIVE）状态的Master的URL
   */
  private var activeMasterUrl: String = ""
  /**
   * 处于激活（ALIVE）状态的Master的WebUI的URL。
   */
  private[worker] var activeMasterWebUiUrl : String = ""
  /***
   * Worker的WebUI的URL。
   */
  private var workerWebUiUrl: String = ""
  // Worker的URL，格式为spark://$name@${ host}:${ port}。
  private val workerUri = RpcEndpointAddress(rpcEnv.address, endpointName).toString
  /**
   * 标记Worker是否已经注册到Master。
   */
  private var registered = false
  /**
   * 标记Worker是否已经连接到Master。
   */
  private var connected = false
  // worker的ID,就是注册host和port的系统时间
  private val workerId = generateWorkerId()
  // 环境变量SPARK_HOME的值。
  private val sparkHome =
    if (testing) {
      assert(sys.props.contains("spark.test.home"), "spark.test.home is not set!")
      new File(sys.props("spark.test.home"))
    } else {
      new File(sys.env.get("SPARK_HOME").getOrElse("."))
    }

  /**
   * 由java.io.File表示的Worker的工作目录。
   */
  var workDir: File = null
  // 已经完成的Executor的身份标识与ExecutorRunner之间的映射关系。
  val finishedExecutors = new LinkedHashMap[String, ExecutorRunner]
  /**
   * Driver的身份标识与DriverRunner之间的映射关系。
   */
  val drivers = new HashMap[String, DriverRunner]
  /**
   * Executor的身份标识与ExecutorRunner之间的映射关系。
   */
  val executors = new HashMap[String, ExecutorRunner]
  /**
   * 已经完成的Driver的身份标识与DriverRunner之间的映射关系。
   */
  val finishedDrivers = new LinkedHashMap[String, DriverRunner]
  /**
   * Application的ID与对应的目录集合之间的映射关系。
   */
  val appDirectories = new HashMap[String, Seq[String]]
  /**
   * 已经完成的Application的ID的集合。
   */
  val finishedApps = new HashSet[String]

//  webUI中保存executor数量,默认1000
  val retainedExecutors = conf.getInt("spark.worker.ui.retainedExecutors",
    WorkerWebUI.DEFAULT_RETAINED_EXECUTORS)
//  webUI中保存Driver程序数量,默认1000
  val retainedDrivers = conf.getInt("spark.worker.ui.retainedDrivers",
    WorkerWebUI.DEFAULT_RETAINED_DRIVERS)

  // The shuffle service is not actually started unless configured.
  /**
   * 外部的Shuffle服务。在6.9节曾经简单介绍过外部Shuffle服务的客户端ExternalShuffleClient。这里则是外部Shuffle服务的服务端实现类
   * External-ShuffleService。这里虽然创建了ExternalShuffleService，但是只有配置了外部的Shuffle服务时，才会启动它
   */
  private val shuffleService = if (externalShuffleServiceSupplier != null) {
    externalShuffleServiceSupplier.get()
  } else {
    new ExternalShuffleService(conf, securityMgr)
  }

  /**
   * Worker的公共地址，如果设置了环境变量SPARK_PUBLIC_DNS，则为环境变量SPARK_PUBLIC_DNS的值，否则为host。
   */
  private val publicAddress = {
    val envVar = conf.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else host
  }
  /**
   * Worker的WebUI，类型为WebUI的子类WorkerWebUI。
   */
  private var webUi: WorkerWebUI = null
  /**
   * 连接尝试次数的计数器。
   */
  private var connectionAttemptCount = 0
  /**
   * 实例名为worker的MetricsSystem，即Worker的度量系统。
   */
  private val metricsSystem = MetricsSystem.createMetricsSystem("worker", conf, securityMgr)
  /**
   * 类型为WorkerSource，是有关Worker的度量来源。
   */
  private val workerSource = new WorkerSource(this)
//  是否启用反向代理,默认关闭
  val reverseProxy = conf.getBoolean("spark.ui.reverseProxy", false)
  /**
   * 由于Worker向Master进行注册的过程是异步的，此变量保存线程返回的java.util.concurrent.Future。
   */
  private var registerMasterFutures: Array[JFuture[_]] = null
  /**
   * Worker向Master进行注册重试的定时器。
   */
  private var registrationRetryTimer: Option[JScheduledFuture[_]] = None

  // A thread pool for registering with masters. Because registering with a master is a blocking
  // action, this thread pool must be able to create "masterRpcAddresses.size" threads at the same
  // time so that we can register with all masters.
//  向多个master同时注册用的线程池
  private val registerMasterThreadPool = ThreadUtils.newDaemonCachedThreadPool(
    "worker-register-master-threadpool",
    masterRpcAddresses.length // Make sure we can register with all masters at the same time
  )
  /**
   * 当前Worker已经使用的内核数。Worker提供了coresFree方法返回cores属性和coresUsed的差值，作为空闲的内核数。
   */
  var coresUsed = 0
  /**
   * 当前Worker已经使用的内存大小。Worker提供了memoryFree方法返回memory属性和memoryUsed的差值，作为空闲的内存大小。
   */
  var memoryUsed = 0

  /**
   * 空闲的CPU核心数
   *
   * @return
   */
  def coresFree: Int = cores - coresUsed

  /**
   * 空闲的内存数
   *
   * @return
   */
  def memoryFree: Int = memory - memoryUsed

  private def createWorkDir() {
//    如果没有设置workDir,这个目录默认在spark目录下创建
    workDir = Option(workDirPath).map(new File(_)).getOrElse(new File(sparkHome, "work"))
    try {
      // This sporadically fails - not sure why ... !workDir.exists() && !workDir.mkdirs()
      // So attempting to create and then check if directory was created or not.
      // 创建目录,如果存在报错退出
      workDir.mkdirs()
      if ( !workDir.exists() || !workDir.isDirectory) {
        logError("Failed to create work directory " + workDir)
        System.exit(1)
      }
      assert (workDir.isDirectory)
    } catch {
      case e: Exception =>
        logError("Failed to create work directory " + workDir, e)
        System.exit(1)
    }
  }

  override def onStart() {
    // 判断registered是否定义
    // 上面初始化了 private var registered = false
    assert(!registered)
    logInfo("Starting Spark worker %s:%d with %d cores, %s RAM".format(
      host, port, cores, Utils.megabytesToString(memory)))
    logInfo(s"Running Spark version ${org.apache.spark.SPARK_VERSION}")
    logInfo("Spark home: " + sparkHome)
    // 调用方法创建WorkerDir
    createWorkDir()
    // 启动shuffle服务
    startExternalShuffleService()
    // 初始化webUI
    webUi = new WorkerWebUI(this, workDir, webUiPort)
    // web设置http服务器
    webUi.bind()

//    绑定webUI端口
    workerWebUiUrl = s"http://$publicAddress:${webUi.boundPort}"
    // 向master注册worker
    registerWithMaster()

    metricsSystem.registerSource(workerSource)
    metricsSystem.start()
    // Attach the worker metrics servlet handler to the web ui after the metrics system is started.
    metricsSystem.getServletHandlers.foreach(webUi.attachHandler)
  }

  /**
   * Change to use the new master.
   *
   * @param masterRef the new master ref
   * @param uiUrl the new master Web UI address
   * @param masterAddress the new master address which the worker should use to connect in case of
   *                      failure
   */
  private def changeMaster(masterRef: RpcEndpointRef, uiUrl: String, masterAddress: RpcAddress) {
    // activeMasterUrl it's a valid Spark url since we receive it from master.
    activeMasterUrl = masterRef.address.toSparkURL
    activeMasterWebUiUrl = uiUrl
    masterAddressToConnect = Some(masterAddress)
    master = Some(masterRef)
    connected = true
    if (reverseProxy) {
      logInfo(s"WorkerWebUI is available at $activeMasterWebUiUrl/proxy/$workerId")
    }
    // Cancel any outstanding re-registration attempts because we found a new master
    cancelLastRegistrationRetry()
  }

  private def tryRegisterAllMasters(): Array[JFuture[_]] = {
    masterRpcAddresses.map { masterAddress =>
      registerMasterThreadPool.submit(new Runnable {
        override def run(): Unit = {
          try {
            logInfo("Connecting to master " + masterAddress + "...")
            val masterEndpoint = rpcEnv.setupEndpointRef(masterAddress, Master.ENDPOINT_NAME)
            sendRegisterMessageToMaster(masterEndpoint)
          } catch {
            case ie: InterruptedException => // Cancelled
            case NonFatal(e) => logWarning(s"Failed to connect to master $masterAddress", e)
          }
        }
      })
    }
  }

  /**
   * Re-register with the master because a network failure or a master failure has occurred.
   * If the re-registration attempt threshold is exceeded, the worker exits with error.
   * Note that for thread-safety this should only be called from the rpcEndpoint.
   */
  private def reregisterWithMaster(): Unit = {
    Utils.tryOrExit {
      connectionAttemptCount += 1
      if (registered) {
        cancelLastRegistrationRetry()
      } else if (connectionAttemptCount <= TOTAL_REGISTRATION_RETRIES) {
        logInfo(s"Retrying connection to master (attempt # $connectionAttemptCount)")
        /**
         * Re-register with the active master this worker has been communicating with. If there
         * is none, then it means this worker is still bootstrapping and hasn't established a
         * connection with a master yet, in which case we should re-register with all masters.
         *
         * It is important to re-register only with the active master during failures. Otherwise,
         * if the worker unconditionally attempts to re-register with all masters, the following
         * race condition may arise and cause a "duplicate worker" error detailed in SPARK-4592:
         *
         *   (1) Master A fails and Worker attempts to reconnect to all masters
         *   (2) Master B takes over and notifies Worker
         *   (3) Worker responds by registering with Master B
         *   (4) Meanwhile, Worker's previous reconnection attempt reaches Master B,
         *       causing the same Worker to register with Master B twice
         *
         * Instead, if we only register with the known active master, we can assume that the
         * old master must have died because another master has taken over. Note that this is
         * still not safe if the old master recovers within this interval, but this is a much
         * less likely scenario.
         */
        master match {
          case Some(masterRef) =>
            // registered == false && master != None means we lost the connection to master, so
            // masterRef cannot be used and we need to recreate it again. Note: we must not set
            // master to None due to the above comments.
            if (registerMasterFutures != null) {
              registerMasterFutures.foreach(_.cancel(true))
            }
            val masterAddress =
              if (preferConfiguredMasterAddress) masterAddressToConnect.get else masterRef.address
            registerMasterFutures = Array(registerMasterThreadPool.submit(new Runnable {
              override def run(): Unit = {
                try {
                  logInfo("Connecting to master " + masterAddress + "...")
                  val masterEndpoint = rpcEnv.setupEndpointRef(masterAddress, Master.ENDPOINT_NAME)
                  sendRegisterMessageToMaster(masterEndpoint)
                } catch {
                  case ie: InterruptedException => // Cancelled
                  case NonFatal(e) => logWarning(s"Failed to connect to master $masterAddress", e)
                }
              }
            }))
          case None =>
            if (registerMasterFutures != null) {
              registerMasterFutures.foreach(_.cancel(true))
            }
            // We are retrying the initial registration
            registerMasterFutures = tryRegisterAllMasters()
        }
        // We have exceeded the initial registration retry threshold
        // All retries from now on should use a higher interval
        if (connectionAttemptCount == INITIAL_REGISTRATION_RETRIES) {
          registrationRetryTimer.foreach(_.cancel(true))
          registrationRetryTimer = Some(
            forwordMessageScheduler.scheduleAtFixedRate(new Runnable {
              override def run(): Unit = Utils.tryLogNonFatalError {
                self.send(ReregisterWithMaster)
              }
            }, PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS,
              PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS,
              TimeUnit.SECONDS))
        }
      } else {
        logError("All masters are unresponsive! Giving up.")
        System.exit(1)
      }
    }
  }

  /**
   * Cancel last registeration retry, or do nothing if no retry
   */
  private def cancelLastRegistrationRetry(): Unit = {
    if (registerMasterFutures != null) {
      registerMasterFutures.foreach(_.cancel(true))
      registerMasterFutures = null
    }
    registrationRetryTimer.foreach(_.cancel(true))
    registrationRetryTimer = None
  }

  private def registerWithMaster() {
    // onDisconnected may be triggered multiple times, so don't attempt registration
    // if there are outstanding registration attempts scheduled.
    registrationRetryTimer match {
      case None =>
        registered = false
        registerMasterFutures = tryRegisterAllMasters()
        connectionAttemptCount = 0
        registrationRetryTimer = Some(forwordMessageScheduler.scheduleAtFixedRate(
          new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
              Option(self).foreach(_.send(ReregisterWithMaster))
            }
          },
          INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS,
          INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS,
          TimeUnit.SECONDS))
      case Some(_) =>
        logInfo("Not spawning another attempt to register with the master, since there is an" +
          " attempt scheduled already.")
    }
  }

  private def startExternalShuffleService() {
    try {
      shuffleService.startIfEnabled()
    } catch {
      case e: Exception =>
        logError("Failed to start external shuffle service", e)
        System.exit(1)
    }
  }

  private def sendRegisterMessageToMaster(masterEndpoint: RpcEndpointRef): Unit = {
    masterEndpoint.send(RegisterWorker(
      workerId,
      host,
      port,
      self,
      cores,
      memory,
      workerWebUiUrl,
      masterEndpoint.address))
  }

  private def handleRegisterResponse(msg: RegisterWorkerResponse): Unit = synchronized {
    msg match {
      case RegisteredWorker(masterRef, masterWebUiUrl, masterAddress) =>
        if (preferConfiguredMasterAddress) {
          logInfo("Successfully registered with master " + masterAddress.toSparkURL)
        } else {
          logInfo("Successfully registered with master " + masterRef.address.toSparkURL)
        }
        registered = true
        changeMaster(masterRef, masterWebUiUrl, masterAddress)
        forwordMessageScheduler.scheduleAtFixedRate(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError {
            self.send(SendHeartbeat)
          }
        }, 0, HEARTBEAT_MILLIS, TimeUnit.MILLISECONDS)
        if (CLEANUP_ENABLED) {
          logInfo(
            s"Worker cleanup enabled; old application directories will be deleted in: $workDir")
          forwordMessageScheduler.scheduleAtFixedRate(new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
              self.send(WorkDirCleanup)
            }
          }, CLEANUP_INTERVAL_MILLIS, CLEANUP_INTERVAL_MILLIS, TimeUnit.MILLISECONDS)
        }

        val execs = executors.values.map { e =>
          new ExecutorDescription(e.appId, e.execId, e.cores, e.state)
        }
        masterRef.send(WorkerLatestState(workerId, execs.toList, drivers.keys.toSeq))

      case RegisterWorkerFailed(message) =>
        if (!registered) {
          logError("Worker registration failed: " + message)
          System.exit(1)
        }

      case MasterInStandby =>
        // Ignore. Master not yet ready.
    }
  }

  override def receive: PartialFunction[Any, Unit] = synchronized {
    case msg: RegisterWorkerResponse =>
      handleRegisterResponse(msg)

    case SendHeartbeat =>
      if (connected) { sendToMaster(Heartbeat(workerId, self)) }

    case WorkDirCleanup =>
      // Spin up a separate thread (in a future) to do the dir cleanup; don't tie up worker
      // rpcEndpoint.
      // Copy ids so that it can be used in the cleanup thread.
      val appIds = (executors.values.map(_.appId) ++ drivers.values.map(_.driverId)).toSet
      try {
        val cleanupFuture: concurrent.Future[Unit] = concurrent.Future {
          val appDirs = workDir.listFiles()
          if (appDirs == null) {
            throw new IOException("ERROR: Failed to list files in " + appDirs)
          }
          appDirs.filter { dir =>
            // the directory is used by an application - check that the application is not running
            // when cleaning up
            val appIdFromDir = dir.getName
            val isAppStillRunning = appIds.contains(appIdFromDir)
            dir.isDirectory && !isAppStillRunning &&
              !Utils.doesDirectoryContainAnyNewFiles(dir, APP_DATA_RETENTION_SECONDS)
          }.foreach { dir =>
            logInfo(s"Removing directory: ${dir.getPath}")
            Utils.deleteRecursively(dir)
          }
        }(cleanupThreadExecutor)

        cleanupFuture.failed.foreach(e =>
          logError("App dir cleanup failed: " + e.getMessage, e)
        )(cleanupThreadExecutor)
      } catch {
        case _: RejectedExecutionException if cleanupThreadExecutor.isShutdown =>
          logWarning("Failed to cleanup work dir as executor pool was shutdown")
      }

    case MasterChanged(masterRef, masterWebUiUrl) =>
      logInfo("Master has changed, new master is at " + masterRef.address.toSparkURL)
      changeMaster(masterRef, masterWebUiUrl, masterRef.address)

      val execs = executors.values.
        map(e => new ExecutorDescription(e.appId, e.execId, e.cores, e.state))
      masterRef.send(WorkerSchedulerStateResponse(workerId, execs.toList, drivers.keys.toSeq))

    case ReconnectWorker(masterUrl) =>
      logInfo(s"Master with url $masterUrl requested this worker to reconnect.")
      registerWithMaster()

    case LaunchExecutor(masterUrl, appId, execId, appDesc, cores_, memory_) =>
      if (masterUrl != activeMasterUrl) {
        logWarning("Invalid Master (" + masterUrl + ") attempted to launch executor.")
      } else {
        try {
          logInfo("Asked to launch executor %s/%d for %s".format(appId, execId, appDesc.name))

          // Create the executor's working directory
          val executorDir = new File(workDir, appId + "/" + execId)
          if (!executorDir.mkdirs()) {
            throw new IOException("Failed to create directory " + executorDir)
          }

          // Create local dirs for the executor. These are passed to the executor via the
          // SPARK_EXECUTOR_DIRS environment variable, and deleted by the Worker when the
          // application finishes.
          val appLocalDirs = appDirectories.getOrElse(appId, {
            val localRootDirs = Utils.getOrCreateLocalRootDirs(conf)
            val dirs = localRootDirs.flatMap { dir =>
              try {
                val appDir = Utils.createDirectory(dir, namePrefix = "executor")
                Utils.chmod700(appDir)
                Some(appDir.getAbsolutePath())
              } catch {
                case e: IOException =>
                  logWarning(s"${e.getMessage}. Ignoring this directory.")
                  None
              }
            }.toSeq
            if (dirs.isEmpty) {
              throw new IOException("No subfolder can be created in " +
                s"${localRootDirs.mkString(",")}.")
            }
            dirs
          })
          appDirectories(appId) = appLocalDirs
          val manager = new ExecutorRunner(
            appId,
            execId,
            appDesc.copy(command = Worker.maybeUpdateSSLSettings(appDesc.command, conf)),
            cores_,
            memory_,
            self,
            workerId,
            host,
            webUi.boundPort,
            publicAddress,
            sparkHome,
            executorDir,
            workerUri,
            conf,
            appLocalDirs, ExecutorState.RUNNING)
          executors(appId + "/" + execId) = manager
          manager.start()
          coresUsed += cores_
          memoryUsed += memory_
          sendToMaster(ExecutorStateChanged(appId, execId, manager.state, None, None))
        } catch {
          case e: Exception =>
            logError(s"Failed to launch executor $appId/$execId for ${appDesc.name}.", e)
            if (executors.contains(appId + "/" + execId)) {
              executors(appId + "/" + execId).kill()
              executors -= appId + "/" + execId
            }
            sendToMaster(ExecutorStateChanged(appId, execId, ExecutorState.FAILED,
              Some(e.toString), None))
        }
      }

    case executorStateChanged @ ExecutorStateChanged(appId, execId, state, message, exitStatus) =>
      handleExecutorStateChanged(executorStateChanged)

    case KillExecutor(masterUrl, appId, execId) =>
      if (masterUrl != activeMasterUrl) {
        logWarning("Invalid Master (" + masterUrl + ") attempted to kill executor " + execId)
      } else {
        val fullId = appId + "/" + execId
        executors.get(fullId) match {
          case Some(executor) =>
            logInfo("Asked to kill executor " + fullId)
            executor.kill()
          case None =>
            logInfo("Asked to kill unknown executor " + fullId)
        }
      }

    case LaunchDriver(driverId, driverDesc) =>
      logInfo(s"Asked to launch driver $driverId")
      val driver = new DriverRunner(
        conf,
        driverId,
        workDir,
        sparkHome,
        driverDesc.copy(command = Worker.maybeUpdateSSLSettings(driverDesc.command, conf)),
        self,
        workerUri,
        securityMgr)
      drivers(driverId) = driver
      driver.start()

      coresUsed += driverDesc.cores
      memoryUsed += driverDesc.mem

    case KillDriver(driverId) =>
      logInfo(s"Asked to kill driver $driverId")
      drivers.get(driverId) match {
        case Some(runner) =>
          runner.kill()
        case None =>
          logError(s"Asked to kill unknown driver $driverId")
      }

    case driverStateChanged @ DriverStateChanged(driverId, state, exception) =>
      handleDriverStateChanged(driverStateChanged)

    case ReregisterWithMaster =>
      reregisterWithMaster()

    case ApplicationFinished(id) =>
      finishedApps += id
      maybeCleanupApplication(id)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RequestWorkerState =>
      context.reply(WorkerStateResponse(host, port, workerId, executors.values.toList,
        finishedExecutors.values.toList, drivers.values.toList,
        finishedDrivers.values.toList, activeMasterUrl, cores, memory,
        coresUsed, memoryUsed, activeMasterWebUiUrl))
  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    if (master.exists(_.address == remoteAddress) ||
      masterAddressToConnect.exists(_ == remoteAddress)) {
      logInfo(s"$remoteAddress Disassociated !")
      masterDisconnected()
    }
  }

  private def masterDisconnected() {
    logError("Connection to master failed! Waiting for master to reconnect...")
    connected = false
    registerWithMaster()
  }

  private def maybeCleanupApplication(id: String): Unit = {
    val shouldCleanup = finishedApps.contains(id) && !executors.values.exists(_.appId == id)
    if (shouldCleanup) {
      finishedApps -= id
      try {
        appDirectories.remove(id).foreach { dirList =>
          concurrent.Future {
            logInfo(s"Cleaning up local directories for application $id")
            dirList.foreach { dir =>
              Utils.deleteRecursively(new File(dir))
            }
          }(cleanupThreadExecutor).failed.foreach(e =>
            logError(s"Clean up app dir $dirList failed: ${e.getMessage}", e)
          )(cleanupThreadExecutor)
        }
      } catch {
        case _: RejectedExecutionException if cleanupThreadExecutor.isShutdown =>
          logWarning("Failed to cleanup application as executor pool was shutdown")
      }
      shuffleService.applicationRemoved(id)
    }
  }

  /**
   * Send a message to the current master. If we have not yet registered successfully with any
   * master, the message will be dropped.
   */
  private def sendToMaster(message: Any): Unit = {
    master match {
      case Some(masterRef) => masterRef.send(message)
      case None =>
        logWarning(
          s"Dropping $message because the connection to master has not yet been established")
    }
  }

  private def generateWorkerId(): String = {
    "worker-%s-%s-%d".format(createDateFormat.format(new Date), host, port)
  }

  override def onStop() {
    cleanupThreadExecutor.shutdownNow()
    metricsSystem.report()
    cancelLastRegistrationRetry()
    forwordMessageScheduler.shutdownNow()
    registerMasterThreadPool.shutdownNow()
    executors.values.foreach(_.kill())
    drivers.values.foreach(_.kill())
    shuffleService.stop()
    webUi.stop()
    metricsSystem.stop()
  }

  private def trimFinishedExecutorsIfNecessary(): Unit = {
    // do not need to protect with locks since both WorkerPage and Restful server get data through
    // thread-safe RpcEndPoint
    if (finishedExecutors.size > retainedExecutors) {
      finishedExecutors.take(math.max(finishedExecutors.size / 10, 1)).foreach {
        case (executorId, _) => finishedExecutors.remove(executorId)
      }
    }
  }

  private def trimFinishedDriversIfNecessary(): Unit = {
    // do not need to protect with locks since both WorkerPage and Restful server get data through
    // thread-safe RpcEndPoint
    if (finishedDrivers.size > retainedDrivers) {
      finishedDrivers.take(math.max(finishedDrivers.size / 10, 1)).foreach {
        case (driverId, _) => finishedDrivers.remove(driverId)
      }
    }
  }

  private[worker] def handleDriverStateChanged(driverStateChanged: DriverStateChanged): Unit = {
    val driverId = driverStateChanged.driverId
    val exception = driverStateChanged.exception
    val state = driverStateChanged.state
    state match {
      case DriverState.ERROR =>
        logWarning(s"Driver $driverId failed with unrecoverable exception: ${exception.get}")
      case DriverState.FAILED =>
        logWarning(s"Driver $driverId exited with failure")
      case DriverState.FINISHED =>
        logInfo(s"Driver $driverId exited successfully")
      case DriverState.KILLED =>
        logInfo(s"Driver $driverId was killed by user")
      case _ =>
        logDebug(s"Driver $driverId changed state to $state")
    }
    sendToMaster(driverStateChanged)
    val driver = drivers.remove(driverId).get
    finishedDrivers(driverId) = driver
    trimFinishedDriversIfNecessary()
    memoryUsed -= driver.driverDesc.mem
    coresUsed -= driver.driverDesc.cores
  }

  private[worker] def handleExecutorStateChanged(executorStateChanged: ExecutorStateChanged):
    Unit = {
    sendToMaster(executorStateChanged)
    val state = executorStateChanged.state
    if (ExecutorState.isFinished(state)) {
      val appId = executorStateChanged.appId
      val fullId = appId + "/" + executorStateChanged.execId
      val message = executorStateChanged.message
      val exitStatus = executorStateChanged.exitStatus
      executors.get(fullId) match {
        case Some(executor) =>
          logInfo("Executor " + fullId + " finished with state " + state +
            message.map(" message " + _).getOrElse("") +
            exitStatus.map(" exitStatus " + _).getOrElse(""))
          executors -= fullId
          finishedExecutors(fullId) = executor
          trimFinishedExecutorsIfNecessary()
          coresUsed -= executor.cores
          memoryUsed -= executor.memory
          if (CLEANUP_NON_SHUFFLE_FILES_ENABLED) {
            shuffleService.executorRemoved(executorStateChanged.execId.toString, appId)
          }
        case None =>
          logInfo("Unknown Executor " + fullId + " finished with state " + state +
            message.map(" message " + _).getOrElse("") +
            exitStatus.map(" exitStatus " + _).getOrElse(""))
      }
      maybeCleanupApplication(appId)
    }
  }
}

private[deploy] object Worker extends Logging {
  val SYSTEM_NAME = "sparkWorker"
  val ENDPOINT_NAME = "Worker"
  private val SSL_NODE_LOCAL_CONFIG_PATTERN = """\-Dspark\.ssl\.useNodeLocalConf\=(.+)""".r

//  参数:--webui-port 8081 spark://host:7077
  def main(argStrings: Array[String]) {
    Thread.setDefaultUncaughtExceptionHandler(new SparkUncaughtExceptionHandler(
      exitOnUncaughtException = false))
    Utils.initDaemon(log)
    // 加载默认配置
    val conf = new SparkConf
//    加载参数和配置进行解析,得到启动RPC和worker需要的参数
    // worker-host、worker-port、webUI-port、cores、memory、master、workerDir、properties
    val args = new WorkerArguments(argStrings, conf)
    // 启动RPC和work终端
    val rpcEnv = startRpcEnvAndEndpoint(args.host, args.port, args.webUiPort, args.cores,
      args.memory, args.masters, args.workDir, conf = conf)
    // With external shuffle service enabled, if we request to launch multiple workers on one host,
    // we can only successfully launch the first worker and the rest fails, because with the port
    // bound, we may launch no more than one external shuffle service on each host.
    // When this happens, we should give explicit reason of failure instead of fail silently. For
    // more detail see SPARK-20989.
//    如果开启shuffle服务,只能启动一台worker,因为会绑定端口,导致其他多台worker启动失败,所以这里关闭
    val externalShuffleServiceEnabled = conf.get(config.SHUFFLE_SERVICE_ENABLED)
//    worker的实例数
    val sparkWorkerInstances = scala.sys.env.getOrElse("SPARK_WORKER_INSTANCES", "1").toInt
//    判断shuffle状态和worker实例个数,有误则提示
    require(externalShuffleServiceEnabled == false || sparkWorkerInstances <= 1,
      "Starting multiple workers on one host is failed because we may launch no more than one " +
        "external shuffle service on each host, please set spark.shuffle.service.enabled to " +
        "false or set SPARK_WORKER_INSTANCES to 1 to resolve the conflict.")
    // RPC启动后处理等待worker注册状态
    rpcEnv.awaitTermination()
  }

// 这里主要初始化了securityManager，RPC，以及注册Worker到RPC，并在执行rpcEnv.setupEndpoint()时，new Worker进行Worker实例化。
  // 传入上面的参数,启动RPC和worker终端
  // 例如在本地启动的,这里传入参数为:
  // host=192.168.2.1
  // port=0
  // webUIport=8081
  // cores=8
  // memory=7050
  // masterUrls=Array(spark://192.168.2.1:7077)
  // workerDir=null
  // properties=null
  def startRpcEnvAndEndpoint(
      host: String,
      port: Int,
      webUiPort: Int,
      cores: Int,
      memory: Int,
      masterUrls: Array[String],
      workDir: String,
      workerNumber: Option[Int] = None,
      conf: SparkConf = new SparkConf): RpcEnv = {

    // The LocalSparkCluster runs multiple local sparkWorkerX RPC Environments
    /**
     * 生成Worker的RpcEnv的名称。生成规则为SYSTEM_NAME后跟Worker号。由于常量SYSTEM_NAME的值为sparkWorker，所以Worker的RpcEnv的名称
     * 将会是spark-Worker0、sparkWorker1等。
     */
    val systemName = SYSTEM_NAME + workerNumber.map(_.toString).getOrElse("")
    // 初始化securityManager,后续另外解析securityManager组件
    val securityMgr = new SecurityManager(conf)
    // 初始化Rpc容器
    val rpcEnv = RpcEnv.create(systemName, host, port, conf, securityMgr)
    // 这里解析了master的host和port
    val masterAddresses = masterUrls.map(RpcAddress.fromSparkURL(_))
    // 将worker注册到RPC,并真正实例化
    rpcEnv.setupEndpoint(ENDPOINT_NAME, new Worker(rpcEnv, webUiPort, cores, memory,
      masterAddresses, ENDPOINT_NAME, workDir, conf, securityMgr))
    // 返回初始化好的rpcEnv
    rpcEnv
  }

  def isUseLocalNodeSSLConfig(cmd: Command): Boolean = {
    val result = cmd.javaOpts.collectFirst {
      case SSL_NODE_LOCAL_CONFIG_PATTERN(_result) => _result.toBoolean
    }
    result.getOrElse(false)
  }

  def maybeUpdateSSLSettings(cmd: Command, conf: SparkConf): Command = {
    val prefix = "spark.ssl."
    val useNLC = "spark.ssl.useNodeLocalConf"
    if (isUseLocalNodeSSLConfig(cmd)) {
      val newJavaOpts = cmd.javaOpts
          .filter(opt => !opt.startsWith(s"-D$prefix")) ++
          conf.getAll.collect { case (key, value) if key.startsWith(prefix) => s"-D$key=$value" } :+
          s"-D$useNLC=true"
      cmd.copy(javaOpts = newJavaOpts)
    } else {
      cmd
    }
  }
}
