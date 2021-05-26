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

package org.apache.spark.deploy

import java.io._
import java.lang.reflect.{InvocationTargetException, Modifier, UndeclaredThrowableException}
import java.net.URL
import java.security.PrivilegedExceptionAction
import java.text.ParseException
import java.util.UUID

import scala.annotation.tailrec
import scala.collection.mutable.{ArrayBuffer, HashMap, Map}
import scala.util.{Properties, Try}

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.ivy.Ivy
import org.apache.ivy.core.LogOptions
import org.apache.ivy.core.module.descriptor._
import org.apache.ivy.core.module.id.{ArtifactId, ModuleId, ModuleRevisionId}
import org.apache.ivy.core.report.ResolveReport
import org.apache.ivy.core.resolve.ResolveOptions
import org.apache.ivy.core.retrieve.RetrieveOptions
import org.apache.ivy.core.settings.IvySettings
import org.apache.ivy.plugins.matcher.GlobPatternMatcher
import org.apache.ivy.plugins.repository.file.FileRepository
import org.apache.ivy.plugins.resolver.{ChainResolver, FileSystemResolver, IBiblioResolver}

import org.apache.spark._
import org.apache.spark.api.r.RUtils
import org.apache.spark.deploy.rest._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.util._

/**
 * Whether to submit, kill, or request the status of an application.
 * The latter two operations are currently supported only for standalone and Mesos cluster modes.
 */
private[deploy] object SparkSubmitAction extends Enumeration {
  type SparkSubmitAction = Value
  val SUBMIT, KILL, REQUEST_STATUS, PRINT_VERSION = Value
}

/**
 * Main gateway of launching a Spark application.
 *
 * This program handles setting up the classpath with relevant Spark dependencies and provides
 * a layer over the different cluster managers and deploy modes that Spark supports.
 */
private[spark] class SparkSubmit extends Logging {

  import DependencyUtils._
  import SparkSubmit._
  // 执行Submit方法
  def doSubmit(args: Array[String]): Unit = {
    // Initialize logging if it hasn't been done yet. Keep track of whether logging needs to
    // be reset before the application starts.
//    初始化logging系统，并跟日志判断是否需要在app启动时重启
    val uninitLog = initializeLogIfNecessary(true, silent = true)
//    调用parseArguments()解析参数，解析了提交的参数及spark配置文件
    val appArgs = parseArguments(args)
    // 参数不重复则输出配置
    if (appArgs.verbose) {
      logInfo(appArgs.toString)
    }

    // 匹配输入的执行请求,也就是提交,终止,请求状态和打印版本
    // 在解析的时候将执行状态封装到了SparkSubmitAction中,这里进行匹配
    // 如果没有执行状态,则SparkSubmitArguments默认设置为SparkSubmitAction.SUBMIT
    // 这里提交会进入submit()
    appArgs.action match {
      case SparkSubmitAction.SUBMIT => submit(appArgs, uninitLog)
      case SparkSubmitAction.KILL => kill(appArgs)
      case SparkSubmitAction.REQUEST_STATUS => requestStatus(appArgs)
      case SparkSubmitAction.PRINT_VERSION => printVersion()
    }
  }

  /**
   * 解析参数的方法
   * @param args
   * @return
   */
  protected def parseArguments(args: Array[String]): SparkSubmitArguments = {
    new SparkSubmitArguments(args)
  }

  /**
   * 终止Submit,只有Standalone和Mesos模式有用
   * Kill an existing submission using the REST protocol. Standalone and Mesos cluster mode only.
   */
  private def kill(args: SparkSubmitArguments): Unit = {
    new RestSubmissionClient(args.master)
      .killSubmission(args.submissionToKill)
  }

  /**
   *  请求状态,只有Standalone和Mesos模式有用
   * Request the status of an existing submission using the REST protocol.
   * Standalone and Mesos cluster mode only.
   */
  private def requestStatus(args: SparkSubmitArguments): Unit = {
    new RestSubmissionClient(args.master)
      .requestSubmissionStatus(args.submissionToRequestStatusFor)
  }

  /** Print version information to the log. */
  // 打印版本信息
  private def printVersion(): Unit = {
    logInfo("""Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version %s
      /_/
                        """.format(SPARK_VERSION))
    logInfo("Using Scala %s, %s, %s".format(
      Properties.versionString, Properties.javaVmName, Properties.javaVersion))
    logInfo(s"Branch $SPARK_BRANCH")
    logInfo(s"Compiled by user $SPARK_BUILD_USER on $SPARK_BUILD_DATE")
    logInfo(s"Revision $SPARK_REVISION")
    logInfo(s"Url $SPARK_REPO_URL")
    logInfo("Type --help for more information.")
  }

  /**
   * Submit the application using the provided parameters, ensuring to first wrap
   * in a doAs when --proxy-user is specified.
   */
  @tailrec
  private def submit(args: SparkSubmitArguments, uninitLog: Boolean): Unit = {

    def doRunMain(): Unit = {
      // 提交时可以指定--proxy-user,如果没有指定,则获取当前用户
      if (args.proxyUser != null) {
        val proxyUser = UserGroupInformation.createProxyUser(args.proxyUser,
          UserGroupInformation.getCurrentUser())
        try {
          proxyUser.doAs(new PrivilegedExceptionAction[Unit]() {
            override def run(): Unit = {
              // 执行runMain()
              runMain(args, uninitLog)
            }
          })
        } catch {
          case e: Exception =>
            // Hadoop's AuthorizationException suppresses the exception's stack trace, which
            // makes the message printed to the output by the JVM not very helpful. Instead,
            // detect exceptions with empty stack traces here, and treat them differently.
            if (e.getStackTrace().length == 0) {
              error(s"ERROR: ${e.getClass().getName()}: ${e.getMessage()}")
            } else {
              throw e
            }
        }
        // 定义了用户则直接执行runMain()
      } else {
        // 启动main后重新初始化logging
        runMain(args, uninitLog)
      }
    }

    // In standalone cluster mode, there are two submission gateways:
    //   (1) The traditional RPC gateway using o.a.s.deploy.Client as a wrapper
    //   (2) The new REST-based gateway introduced in Spark 1.3
    // The latter is the default behavior as of Spark 1.3, but Spark submit will fail over
    // to use the legacy gateway if the master endpoint turns out to be not a REST server.
    // standalone模式有两种提交网关,
    // 使用o.a.s.apply.client作为包装器的传统RPC网关和基于REST服务的网关,spark1.3后默认使用REST
    // 如果master终端没有使用REST服务,spark会故障切换到RPC
    // 这里判断standalone模式和使用REST服务
    if (args.isStandaloneCluster && args.useRest) {
      try {
        logInfo("Running Spark using the REST application submission protocol.")
        doRunMain()
      } catch {
        // Fail over to use the legacy submission gateway
        case e: SubmitRestConnectionException =>
          logWarning(s"Master endpoint ${args.master} was not a REST server. " +
            "Falling back to legacy submission gateway instead.")
          args.useRest = false
          submit(args, false)
      }
    // In all other modes, just run the main class as prepared
    } else {
      // 其他模式,按准备的环境调用上面的doRunMain()运行相应的main()
      // 在进入前,初始化了SparkContext和SparkSession
      doRunMain()
    }
  }

  /**
   *
   * SparkSubmit的运行环境准备方法
   * args: 通过SparkSubmitArguments解析的参数
   * conf：hadoop的conf设置，只在测试的时候使用
   * 返回元组(子类参数，子类路径，k/v格式spark配置，子类Main)
   *
   * Prepare the environment for submitting an application.
   *
   * @param args the parsed SparkSubmitArguments used for environment preparation.
   * @param conf the Hadoop Configuration, this argument will only be set in unit test.
   * @return a 4-tuple:
   *        (1) the arguments for the child process,
   *        (2) a list of classpath entries for the child,
   *        (3) a map of system properties, and
   *        (4) the main class for the child
   *
   * Exposed for testing.
   */
  private[deploy] def prepareSubmitEnvironment(
      args: SparkSubmitArguments,
      conf: Option[HadoopConfiguration] = None)
      : (Seq[String], Seq[String], SparkConf, String) = {
    // Return values
    val childArgs = new ArrayBuffer[String]()
    val childClasspath = new ArrayBuffer[String]()
    val sparkConf = new SparkConf()
    var childMainClass = ""

    // Set the cluster manager
    // 集群管理器
    // 也就是提交时指定--master local/yarn/yarn-client/yarn-cluster/spark://192.168.2.1:7077
    // 或者 mesos,k8s等运行模式
    val clusterManager: Int = args.master match {
      case "yarn" => YARN
      case "yarn-client" | "yarn-cluster" =>
        logWarning(s"Master ${args.master} is deprecated since 2.0." +
          " Please use master \"yarn\" with specified deploy mode instead.")
        YARN
      case m if m.startsWith("spark") => STANDALONE
      case m if m.startsWith("mesos") => MESOS
      case m if m.startsWith("k8s") => KUBERNETES
      case m if m.startsWith("local") => LOCAL
      case _ =>
        error("Master must either be yarn or start with spark, mesos, k8s, or local")
        -1
    }

    // Set the deploy mode; default is client mode
    var deployMode: Int = args.deployMode match {
      case "client" | null => CLIENT
      case "cluster" => CLUSTER
      case _ =>
        error("Deploy mode must be either client or cluster")
        -1
    }

    // 判断--deploy-mode的模式
    // 如果没有设置,默认为client模式,--master yarn提交,不加--deploy-mode clinet其实也是yarn-client模式
    // 设置了cluster才会以yarn-cluster模式运行
    // Because the deprecated way of specifying "yarn-cluster" and "yarn-client" encapsulate both
    // the master and deploy mode, we have some logic to infer the master and deploy mode
    // from each other if only one is specified, or exit early if they are at odds.
    if (clusterManager == YARN) {
      (args.master, args.deployMode) match {
        case ("yarn-cluster", null) =>
          deployMode = CLUSTER
          args.master = "yarn"
        case ("yarn-cluster", "client") =>
          error("Client deploy mode is not compatible with master \"yarn-cluster\"")
        case ("yarn-client", "cluster") =>
          error("Cluster deploy mode is not compatible with master \"yarn-client\"")
        case (_, mode) =>
          args.master = "yarn"
      }

      // Make sure YARN is included in our build if we're trying to use it
      if (!Utils.classIsLoadable(YARN_CLUSTER_SUBMIT_CLASS) && !Utils.isTesting) {
        error(
          "Could not load YARN classes. " +
          "This copy of Spark may not have been compiled with YARN support.")
      }
    }

    if (clusterManager == KUBERNETES) {
      args.master = Utils.checkAndGetK8sMasterUrl(args.master)
      // Make sure KUBERNETES is included in our build if we're trying to use it
      if (!Utils.classIsLoadable(KUBERNETES_CLUSTER_SUBMIT_CLASS) && !Utils.isTesting) {
        error(
          "Could not load KUBERNETES classes. " +
            "This copy of Spark may not have been compiled with KUBERNETES support.")
      }
    }

    // Fail fast, the following modes are not supported or applicable
    (clusterManager, deployMode) match {
      case (STANDALONE, CLUSTER) if args.isPython =>
        error("Cluster deploy mode is currently not supported for python " +
          "applications on standalone clusters.")
      case (STANDALONE, CLUSTER) if args.isR =>
        error("Cluster deploy mode is currently not supported for R " +
          "applications on standalone clusters.")
      case (LOCAL, CLUSTER) =>
        error("Cluster deploy mode is not compatible with master \"local\"")
      case (_, CLUSTER) if isShell(args.primaryResource) =>
        error("Cluster deploy mode is not applicable to Spark shells.")
      case (_, CLUSTER) if isSqlShell(args.mainClass) =>
        error("Cluster deploy mode is not applicable to Spark SQL shell.")
      case (_, CLUSTER) if isThriftServer(args.mainClass) =>
        error("Cluster deploy mode is not applicable to Spark Thrift server.")
      case _ =>
    }

    // Update args.deployMode if it is null. It will be passed down as a Spark property later.
    (args.deployMode, deployMode) match {
      case (null, CLIENT) => args.deployMode = "client"
      case (null, CLUSTER) => args.deployMode = "cluster"
      case _ =>
    }
    val isYarnCluster = clusterManager == YARN && deployMode == CLUSTER
    val isMesosCluster = clusterManager == MESOS && deployMode == CLUSTER
    val isStandAloneCluster = clusterManager == STANDALONE && deployMode == CLUSTER
    val isKubernetesCluster = clusterManager == KUBERNETES && deployMode == CLUSTER
    val isMesosClient = clusterManager == MESOS && deployMode == CLIENT

    // 主要是添加依赖
    if (!isMesosCluster && !isStandAloneCluster) {
      // Resolve maven dependencies if there are any and add classpath to jars. Add them to py-files
      // too for packages that include Python code
      val resolvedMavenCoordinates = DependencyUtils.resolveMavenDependencies(
        args.packagesExclusions,
        args.packages,
        args.repositories,
        args.ivyRepoPath,
        args.ivySettingsPath)

      if (!StringUtils.isBlank(resolvedMavenCoordinates)) {
        args.jars = mergeFileLists(args.jars, resolvedMavenCoordinates)
        if (args.isPython || isInternal(args.primaryResource)) {
          args.pyFiles = mergeFileLists(args.pyFiles, resolvedMavenCoordinates)
        }
      }

      // install any R packages that may have been passed through --jars or --packages.
      // Spark Packages may contain R source code inside the jar.
      if (args.isR && !StringUtils.isBlank(args.jars)) {
        RPackageUtils.checkAndBuildRPackage(args.jars, printStream, args.verbose)
      }
    }
    // 将args.sparkProperties参数加入sparkConf配置中
    args.sparkProperties.foreach { case (k, v) => sparkConf.set(k, v) }
    // 设置hadoop配置,如果为空,加载spark配置
    val hadoopConf = conf.getOrElse(SparkHadoopUtil.newConfiguration(sparkConf))
    // 工作临时目录
    val targetDir = Utils.createTempDir()

    // assure a keytab is available from any place in a JVM
    // 判断当前模式下sparkConf的k/v键值对中key是否在JVM中全局可用
    if (clusterManager == YARN || clusterManager == LOCAL || isMesosClient) {
      if (args.principal != null) {
        if (args.keytab != null) {
          require(new File(args.keytab).exists(), s"Keytab file: ${args.keytab} does not exist")
          // Add keytab and principal configurations in sysProps to make them available
          // for later use; e.g. in spark sql, the isolated class loader used to talk
          // to HiveMetastore will use these settings. They will be set as Java system
          // properties and then loaded by SparkConf
          sparkConf.set(KEYTAB, args.keytab)
          sparkConf.set(PRINCIPAL, args.principal)
          UserGroupInformation.loginUserFromKeytab(args.principal, args.keytab)
        }
      }
    }

    // Resolve glob path for different resources.
    // 设置全局资源,也就是合并各种模式依赖的路径的资源和hadoopConf中设置路径的资源
    // 各种jars,file,pyfile和压缩包
    args.jars = Option(args.jars).map(resolveGlobPaths(_, hadoopConf)).orNull
    args.files = Option(args.files).map(resolveGlobPaths(_, hadoopConf)).orNull
    args.pyFiles = Option(args.pyFiles).map(resolveGlobPaths(_, hadoopConf)).orNull
    args.archives = Option(args.archives).map(resolveGlobPaths(_, hadoopConf)).orNull

    lazy val secMgr = new SecurityManager(sparkConf)

    // In client mode, download remote files.
//    client模式下载资源
//    client模式下,先将需要的远程文件下载到本地
    var localPrimaryResource: String = null
    var localJars: String = null
    var localPyFiles: String = null
    if (deployMode == CLIENT) {
      localPrimaryResource = Option(args.primaryResource).map {
        downloadFile(_, targetDir, sparkConf, hadoopConf, secMgr)
      }.orNull
      localJars = Option(args.jars).map {
        downloadFileList(_, targetDir, sparkConf, hadoopConf, secMgr)
      }.orNull
      localPyFiles = Option(args.pyFiles).map {
        downloadFileList(_, targetDir, sparkConf, hadoopConf, secMgr)
      }.orNull
    }

    /**
     * yarn模式下载资源
     * yarn模式下,hdfs不支持加载到内存,所以采用"spark.yarn.dist.forceDownloadSchemes"方案
     * 所以先把方案列表文件下载到本地,再通过相应方案加载资源到分布式内存中
     * 在yarn-client模式中,上面的代码中已经把远程文件下载到了本地,只需要获取本地路径替换掉远程路径即可
     */
    // When running in YARN, for some remote resources with scheme:
    //   1. Hadoop FileSystem doesn't support them.
    //   2. We explicitly bypass Hadoop FileSystem with "spark.yarn.dist.forceDownloadSchemes".
    // We will download them to local disk prior to add to YARN's distributed cache.
    // For yarn client mode, since we already download them with above code, so we only need to
    // figure out the local path and replace the remote one.
    if (clusterManager == YARN) {
      // 加载方案列表
      val forceDownloadSchemes = sparkConf.get(FORCE_DOWNLOAD_SCHEMES)

      // 判断是否需要下载的方法
      def shouldDownload(scheme: String): Boolean = {
        forceDownloadSchemes.contains("*") || forceDownloadSchemes.contains(scheme) ||
          Try { FileSystem.getFileSystemClass(scheme, hadoopConf) }.isFailure
      }

      // 下载资源的方法
      def downloadResource(resource: String): String = {
        val uri = Utils.resolveURI(resource)
        uri.getScheme match {
          case "local" | "file" => resource
          case e if shouldDownload(e) =>
            val file = new File(targetDir, new Path(uri).getName)
            if (file.exists()) {
              file.toURI.toString
            } else {
              downloadFile(resource, targetDir, sparkConf, hadoopConf, secMgr)
            }
          case _ => uri.toString
        }
      }
      // 下载主要运行资源
      args.primaryResource = Option(args.primaryResource).map { downloadResource }.orNull
      // 下载文件
      args.files = Option(args.files).map { files =>
        Utils.stringToSeq(files).map(downloadResource).mkString(",")
      }.orNull
      args.pyFiles = Option(args.pyFiles).map { pyFiles =>
        Utils.stringToSeq(pyFiles).map(downloadResource).mkString(",")
      }.orNull
      // 下载jars
      args.jars = Option(args.jars).map { jars =>
        Utils.stringToSeq(jars).map(downloadResource).mkString(",")
      }.orNull
      // 下载压缩文件
      args.archives = Option(args.archives).map { archives =>
        Utils.stringToSeq(archives).map(downloadResource).mkString(",")
      }.orNull
    }

    // If we're running a python app, set the main class to our specific python runner
    if (args.isPython && deployMode == CLIENT) {
      if (args.primaryResource == PYSPARK_SHELL) {
        args.mainClass = "org.apache.spark.api.python.PythonGatewayServer"
      } else {
        // If a python file is provided, add it to the child arguments and list of files to deploy.
        // Usage: PythonAppRunner <main python file> <extra python files> [app arguments]
        args.mainClass = "org.apache.spark.deploy.PythonRunner"
        args.childArgs = ArrayBuffer(localPrimaryResource, localPyFiles) ++ args.childArgs
      }
      if (clusterManager != YARN) {
        // The YARN backend handles python files differently, so don't merge the lists.
        args.files = mergeFileLists(args.files, args.pyFiles)
      }
    }

    if (localPyFiles != null) {
      sparkConf.set("spark.submit.pyFiles", localPyFiles)
    }

    // In YARN mode for an R app, add the SparkR package archive and the R package
    // archive containing all of the built R libraries to archives so that they can
    // be distributed with the job
    if (args.isR && clusterManager == YARN) {
      val sparkRPackagePath = RUtils.localSparkRPackagePath
      if (sparkRPackagePath.isEmpty) {
        error("SPARK_HOME does not exist for R application in YARN mode.")
      }
      val sparkRPackageFile = new File(sparkRPackagePath.get, SPARKR_PACKAGE_ARCHIVE)
      if (!sparkRPackageFile.exists()) {
        error(s"$SPARKR_PACKAGE_ARCHIVE does not exist for R application in YARN mode.")
      }
      val sparkRPackageURI = Utils.resolveURI(sparkRPackageFile.getAbsolutePath).toString

      // Distribute the SparkR package.
      // Assigns a symbol link name "sparkr" to the shipped package.
      args.archives = mergeFileLists(args.archives, sparkRPackageURI + "#sparkr")

      // Distribute the R package archive containing all the built R packages.
      if (!RUtils.rPackages.isEmpty) {
        val rPackageFile =
          RPackageUtils.zipRLibraries(new File(RUtils.rPackages.get), R_PACKAGE_ARCHIVE)
        if (!rPackageFile.exists()) {
          error("Failed to zip all the built R packages.")
        }

        val rPackageURI = Utils.resolveURI(rPackageFile.getAbsolutePath).toString
        // Assigns a symbol link name "rpkg" to the shipped package.
        args.archives = mergeFileLists(args.archives, rPackageURI + "#rpkg")
      }
    }

    // TODO: Support distributing R packages with standalone cluster
    if (args.isR && clusterManager == STANDALONE && !RUtils.rPackages.isEmpty) {
      error("Distributing R packages with standalone cluster is not supported.")
    }

    // TODO: Support distributing R packages with mesos cluster
    if (args.isR && clusterManager == MESOS && !RUtils.rPackages.isEmpty) {
      error("Distributing R packages with mesos cluster is not supported.")
    }

    // If we're running an R app, set the main class to our specific R runner
    if (args.isR && deployMode == CLIENT) {
      if (args.primaryResource == SPARKR_SHELL) {
        args.mainClass = "org.apache.spark.api.r.RBackend"
      } else {
        // If an R file is provided, add it to the child arguments and list of files to deploy.
        // Usage: RRunner <main R file> [app arguments]
        args.mainClass = "org.apache.spark.deploy.RRunner"
        args.childArgs = ArrayBuffer(localPrimaryResource) ++ args.childArgs
        args.files = mergeFileLists(args.files, args.primaryResource)
      }
    }

    if (isYarnCluster && args.isR) {
      // In yarn-cluster mode for an R app, add primary resource to files
      // that can be distributed with the job
      args.files = mergeFileLists(args.files, args.primaryResource)
    }

    // Special flag to avoid deprecation warnings at the client
    sys.props("SPARK_SUBMIT") = "true"

    // A list of rules to map each argument to system properties or command-line options in
    // each deploy mode; we iterate through these below
    // 为各种部署模式设置相应参数
    // 这里返回的是元组
    // OptionAssigner类没有方法,只是设置了参数类型
    val options = List[OptionAssigner](

      // All cluster managers
      // 设置所有集群管理的参数属性
      OptionAssigner(args.master, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES, confKey = "spark.master"),
      OptionAssigner(args.deployMode, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES,
        confKey = "spark.submit.deployMode"),
      OptionAssigner(args.name, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES, confKey = "spark.app.name"),
      OptionAssigner(args.ivyRepoPath, ALL_CLUSTER_MGRS, CLIENT, confKey = "spark.jars.ivy"),
      OptionAssigner(args.driverMemory, ALL_CLUSTER_MGRS, CLIENT,
        confKey = "spark.driver.memory"),
      OptionAssigner(args.driverExtraClassPath, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES,
        confKey = "spark.driver.extraClassPath"),
      OptionAssigner(args.driverExtraJavaOptions, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES,
        confKey = "spark.driver.extraJavaOptions"),
      OptionAssigner(args.driverExtraLibraryPath, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES,
        confKey = "spark.driver.extraLibraryPath"),

      // Propagate attributes for dependency resolution at the driver side
      // driver依赖的参数属性
      OptionAssigner(args.packages, STANDALONE | MESOS, CLUSTER, confKey = "spark.jars.packages"),
      OptionAssigner(args.repositories, STANDALONE | MESOS, CLUSTER,
        confKey = "spark.jars.repositories"),
      OptionAssigner(args.ivyRepoPath, STANDALONE | MESOS, CLUSTER, confKey = "spark.jars.ivy"),
      OptionAssigner(args.packagesExclusions, STANDALONE | MESOS,
        CLUSTER, confKey = "spark.jars.excludes"),

      // Yarn only
      // yarn模式时需要的参数属性
      // 任务队列,executor实例,依赖包和文件,运行的用户,args.key列表等
      OptionAssigner(args.queue, YARN, ALL_DEPLOY_MODES, confKey = "spark.yarn.queue"),
      OptionAssigner(args.numExecutors, YARN, ALL_DEPLOY_MODES,
        confKey = "spark.executor.instances"),
      OptionAssigner(args.pyFiles, YARN, ALL_DEPLOY_MODES, confKey = "spark.yarn.dist.pyFiles"),
      OptionAssigner(args.jars, YARN, ALL_DEPLOY_MODES, confKey = "spark.yarn.dist.jars"),
      OptionAssigner(args.files, YARN, ALL_DEPLOY_MODES, confKey = "spark.yarn.dist.files"),
      OptionAssigner(args.archives, YARN, ALL_DEPLOY_MODES, confKey = "spark.yarn.dist.archives"),
      OptionAssigner(args.principal, YARN, ALL_DEPLOY_MODES, confKey = "spark.yarn.principal"),
      OptionAssigner(args.keytab, YARN, ALL_DEPLOY_MODES, confKey = "spark.yarn.keytab"),

      // Other options
      // 其他设置,用的比较多的是yarn的参数属性,当然测试中会使用local和standalone
      OptionAssigner(args.executorCores, STANDALONE | YARN | KUBERNETES, ALL_DEPLOY_MODES,
        confKey = "spark.executor.cores"),
      OptionAssigner(args.executorMemory, STANDALONE | MESOS | YARN | KUBERNETES, ALL_DEPLOY_MODES,
        confKey = "spark.executor.memory"),
      OptionAssigner(args.totalExecutorCores, STANDALONE | MESOS | KUBERNETES, ALL_DEPLOY_MODES,
        confKey = "spark.cores.max"),
      OptionAssigner(args.files, LOCAL | STANDALONE | MESOS | KUBERNETES, ALL_DEPLOY_MODES,
        confKey = "spark.files"),
      OptionAssigner(args.jars, LOCAL, CLIENT, confKey = "spark.jars"),
      OptionAssigner(args.jars, STANDALONE | MESOS | KUBERNETES, ALL_DEPLOY_MODES,
        confKey = "spark.jars"),
      OptionAssigner(args.driverMemory, STANDALONE | MESOS | YARN | KUBERNETES, CLUSTER,
        confKey = "spark.driver.memory"),
      OptionAssigner(args.driverCores, STANDALONE | MESOS | YARN | KUBERNETES, CLUSTER,
        confKey = "spark.driver.cores"),
      OptionAssigner(args.supervise.toString, STANDALONE | MESOS, CLUSTER,
        confKey = "spark.driver.supervise"),
      OptionAssigner(args.ivyRepoPath, STANDALONE, CLUSTER, confKey = "spark.jars.ivy"),

      // An internal option used only for spark-shell to add user jars to repl's classloader,
      // previously it uses "spark.jars" or "spark.yarn.dist.jars" which now may be pointed to
      // remote jars, so adding a new option to only specify local jars for spark-shell internally.
      OptionAssigner(localJars, ALL_CLUSTER_MGRS, CLIENT, confKey = "spark.repl.local.jars")
    )

    // In client mode, launch the application main class directly
    // In addition, add the main application jar and any added jars (if any) to the classpath
    // local模式直接加载子类main class,把资源jars都添加到子类的路径中
    if (deployMode == CLIENT) {
      childMainClass = args.mainClass
      if (localPrimaryResource != null && isUserJar(localPrimaryResource)) {
        childClasspath += localPrimaryResource
      }
      if (localJars != null) { childClasspath ++= localJars.split(",") }
    }
    // Add the main application jar and any added jars to classpath in case YARN client
    // requires these jars.
    // This assumes both primaryResource and user jars are local jars, or already downloaded
    // to local by configuring "spark.yarn.dist.forceDownloadSchemes", otherwise it will not be
    // added to the classpath of YARN client.
    // 加载jars到yarn-client的classpath中
    // 如果通过方案 "spark.yarn.dist.forceDownloadSchemes"已经将资源下载到本地才会被添加
    if (isYarnCluster) {
      if (isUserJar(args.primaryResource)) {
        childClasspath += args.primaryResource
      }
      if (args.jars != null) { childClasspath ++= args.jars.split(",") }
    }

    // client模式,子类参数直接添加下载到本地的参数
    if (deployMode == CLIENT) {
      if (args.childArgs != null) { childArgs ++= args.childArgs }
    }

    // Map all arguments to command-line options or system properties for our chosen mode
    // 将所有参数对应到所选模式的命令行和系统配置中
    for (opt <- options) {
      // 需要判断options非空,deployMode已设置,clusterManager已设置
      if (opt.value != null &&
          (deployMode & opt.deployMode) != 0 &&
          (clusterManager & opt.clusterManager) != 0) {
        if (opt.clOption != null) { childArgs += (opt.clOption, opt.value) }
        if (opt.confKey != null) { sparkConf.set(opt.confKey, opt.value) }
      }
    }

    //spark-shell的webUI设置
    // In case of shells, spark.ui.showConsoleProgress can be true by default or by user.
    if (isShell(args.primaryResource) && !sparkConf.contains(UI_SHOW_CONSOLE_PROGRESS)) {
      sparkConf.set(UI_SHOW_CONSOLE_PROGRESS, true)
    }

    // Add the application jar automatically so the user doesn't have to call sc.addJar
    // For YARN cluster mode, the jar is already distributed on each node as "app.jar"
    // For python and R files, the primary resource is already distributed as a regular file
    // 自动添加jars,不用为yarn-cluster再添加,而python和R的jars已经随job分发到各个节点
    // 所以这里判断不是这三种模式
    // 这里也把主运行程序分发到各节点
    if (!isYarnCluster && !args.isPython && !args.isR) {
      var jars = sparkConf.getOption("spark.jars").map(x => x.split(",").toSeq).getOrElse(Seq.empty)
      if (isUserJar(args.primaryResource)) {
        jars = jars ++ Seq(args.primaryResource)
      }
      sparkConf.set("spark.jars", jars.mkString(","))
    }

    // In standalone cluster mode, use the REST client to submit the application (Spark 1.3+).
    // All Spark parameters are expected to be passed to the client through system properties.
    // standalone cluster模式通过REST客户端提交app,spark参数通过系统配置传递给其他客户端
    if (args.isStandaloneCluster) {
      if (args.useRest) {
        childMainClass = REST_CLUSTER_SUBMIT_CLASS
        childArgs += (args.primaryResource, args.mainClass)
      } else {
        // In legacy standalone cluster mode, use Client as a wrapper around the user class
        // 使用RPC的话,将客户端作为用户类的包装执行器
        childMainClass = STANDALONE_CLUSTER_SUBMIT_CLASS
        if (args.supervise) { childArgs += "--supervise" }
        Option(args.driverMemory).foreach { m => childArgs += ("--memory", m) }
        Option(args.driverCores).foreach { c => childArgs += ("--cores", c) }
        childArgs += "launch"
        childArgs += (args.master, args.primaryResource, args.mainClass)
      }
      if (args.childArgs != null) {
        childArgs ++= args.childArgs
      }
    }

    // Let YARN know it's a pyspark app, so it distributes needed libraries.
    if (clusterManager == YARN) {
      if (args.isPython) {
        sparkConf.set("spark.yarn.isPython", "true")
      }
    }

    if (clusterManager == MESOS && UserGroupInformation.isSecurityEnabled) {
      setRMPrincipal(sparkConf)
    }

    // In yarn-cluster mode, use yarn.Client as a wrapper around the user class
    // yarn-cluster模式,使用yarn.client作为用户提交类的包装执行器
    if (isYarnCluster) {
      childMainClass = YARN_CLUSTER_SUBMIT_CLASS
      // 若是python语言
      if (args.isPython) {
        childArgs += ("--primary-py-file", args.primaryResource)
        childArgs += ("--class", "org.apache.spark.deploy.PythonRunner")
      // 若是R语言
      } else if (args.isR) {
        val mainFile = new Path(args.primaryResource).getName
        childArgs += ("--primary-r-file", mainFile)
        childArgs += ("--class", "org.apache.spark.deploy.RRunner")
      } else {
        // 则是java/ scala
        // 主执行资源不是spark内部资源,就通过--jar把资源添加到子类参数
        if (args.primaryResource != SparkLauncher.NO_RESOURCE) {
          childArgs += ("--jar", args.primaryResource)
        }
        // 已经设置到spark系统配置就直接添加--class指定执行类
        childArgs += ("--class", args.mainClass)
      }
      // 遍历所有args参数,添加到子类参数中
      if (args.childArgs != null) {
        args.childArgs.foreach { arg => childArgs += ("--arg", arg) }
      }
    }
    // mesos和k8s跟上面差不多
    if (isMesosCluster) {
      assert(args.useRest, "Mesos cluster mode is only supported through the REST submission API")
      childMainClass = REST_CLUSTER_SUBMIT_CLASS
      if (args.isPython) {
        // Second argument is main class
        childArgs += (args.primaryResource, "")
        if (args.pyFiles != null) {
          sparkConf.set("spark.submit.pyFiles", args.pyFiles)
        }
      } else if (args.isR) {
        // Second argument is main class
        childArgs += (args.primaryResource, "")
      } else {
        childArgs += (args.primaryResource, args.mainClass)
      }
      if (args.childArgs != null) {
        childArgs ++= args.childArgs
      }
    }

    // k8s的方式
    if (isKubernetesCluster) {
      childMainClass = KUBERNETES_CLUSTER_SUBMIT_CLASS
      if (args.primaryResource != SparkLauncher.NO_RESOURCE) {
        if (args.isPython) {
          childArgs ++= Array("--primary-py-file", args.primaryResource)
          childArgs ++= Array("--main-class", "org.apache.spark.deploy.PythonRunner")
          if (args.pyFiles != null) {
            childArgs ++= Array("--other-py-files", args.pyFiles)
          }
        } else if (args.isR) {
          childArgs ++= Array("--primary-r-file", args.primaryResource)
          childArgs ++= Array("--main-class", "org.apache.spark.deploy.RRunner")
        }
        else {
          childArgs ++= Array("--primary-java-resource", args.primaryResource)
          childArgs ++= Array("--main-class", args.mainClass)
        }
      } else {
        childArgs ++= Array("--main-class", args.mainClass)
      }
      if (args.childArgs != null) {
        args.childArgs.foreach { arg =>
          childArgs += ("--arg", arg)
        }
      }
    }

    // Load any properties specified through --conf and the default properties file
    // 加载spark配置,这个配置是上面解析过的--conf和默认配置文件的配置项
    for ((k, v) <- args.sparkProperties) {
      sparkConf.setIfMissing(k, v)
    }

    // Ignore invalid spark.driver.host in cluster modes.
    // 忽略cluster模式中无效的driver.host,因为cluster模式时driver是运行在worker中
    if (deployMode == CLUSTER) {
      sparkConf.remove("spark.driver.host")
    }

    // Resolve paths in certain spark properties
    // 解析依赖jars和文件的路径
    val pathConfigs = Seq(
      "spark.jars",
      "spark.files",
      "spark.yarn.dist.files",
      "spark.yarn.dist.archives",
      "spark.yarn.dist.jars")

    // 遍历解析的路径,去除空值和不合理的,并解析为新的URI
    pathConfigs.foreach { config =>
      // Replace old URIs with resolved URIs, if they exist
      sparkConf.getOption(config).foreach { oldValue =>
        sparkConf.set(config, Utils.resolveURIs(oldValue))
      }
    }

    // Resolve and format python file paths properly before adding them to the PYTHONPATH.
    // The resolving part is redundant in the case of --py-files, but necessary if the user
    // explicitly sets `spark.submit.pyFiles` in his/her default properties file.
    sparkConf.getOption("spark.submit.pyFiles").foreach { pyFiles =>
      val resolvedPyFiles = Utils.resolveURIs(pyFiles)
      val formattedPyFiles = if (!isYarnCluster && !isMesosCluster) {
        PythonRunner.formatPaths(resolvedPyFiles).mkString(",")
      } else {
        // Ignoring formatting python path in yarn and mesos cluster mode, these two modes
        // support dealing with remote python files, they could distribute and add python files
        // locally.
        resolvedPyFiles
      }
      sparkConf.set("spark.submit.pyFiles", formattedPyFiles)
    }

    (childArgs, childClasspath, sparkConf, childMainClass)
  }

  // [SPARK-20328]. HadoopRDD calls into a Hadoop library that fetches delegation tokens with
  // renewer set to the YARN ResourceManager.  Since YARN isn't configured in Mesos mode, we
  // must trick it into thinking we're YARN.
  private def setRMPrincipal(sparkConf: SparkConf): Unit = {
    val shortUserName = UserGroupInformation.getCurrentUser.getShortUserName
    val key = s"spark.hadoop.${YarnConfiguration.RM_PRINCIPAL}"
    logInfo(s"Setting ${key} to ${shortUserName}")
    sparkConf.set(key, shortUserName)
  }

  /**
   *
   * Run the main method of the child class using the submit arguments.
   *
   * This runs in two steps. First, we prepare the launch environment by setting up
   * the appropriate classpath, system properties, and application arguments for
   * running the child main class based on the cluster manager and the deploy mode.
   * Second, we use this launch environment to invoke the main method of the child
   * main class.
   *
   * Note that this main class will not be the one provided by the user if we're
   * running cluster deploy mode or python applications.
   */
  private def runMain(args: SparkSubmitArguments, uninitLog: Boolean): Unit = {
    val (childArgs, childClasspath, sparkConf, childMainClass) = prepareSubmitEnvironment(args)
    // Let the main class re-initialize the logging system once it starts.
    if (uninitLog) {
      Logging.uninitialize()
    }

    if (args.verbose) {
      logInfo(s"Main class:\n$childMainClass")
      logInfo(s"Arguments:\n${childArgs.mkString("\n")}")
      // sysProps may contain sensitive information, so redact before printing
      logInfo(s"Spark config:\n${Utils.redact(sparkConf.getAll.toMap).mkString("\n")}")
      logInfo(s"Classpath elements:\n${childClasspath.mkString("\n")}")
      logInfo("\n")
    }
    // 初始化类加载器
    val loader =
    // 如果用户设置了class,通过ChildFirstURLClassLoader来加载
      if (sparkConf.get(DRIVER_USER_CLASS_PATH_FIRST)) {
        // 该类可以摆脱双亲委派机制
        new ChildFirstURLClassLoader(new Array[URL](0),
          Thread.currentThread.getContextClassLoader)
      } else {
        // 如果用户没有设置,通过MutableURLClassLoader来加载
        new MutableURLClassLoader(new Array[URL](0),
          Thread.currentThread.getContextClassLoader)
      }
    // 设置由上面自定义的类加载器来加载class到JVM
    Thread.currentThread.setContextClassLoader(loader)
    // 加载jars
    for (jar <- childClasspath) {
      addJarToClasspath(jar, loader)
    }

    // 先定义一个mainlcass变量
    var mainClass: Class[_] = null

    try {
      // 反射加载子类mainclass
      mainClass = Utils.classForName(childMainClass)
    } catch {
      // 如果没找到类,并且类中包含hive的thriftserver,提示需要启动hive服务
      case e: ClassNotFoundException =>
        logWarning(s"Failed to load $childMainClass.", e)
        if (childMainClass.contains("thriftserver")) {
          logInfo(s"Failed to load main class $childMainClass.")
          logInfo("You need to build Spark with -Phive and -Phive-thriftserver.")
        }
        throw new SparkUserAppException(CLASS_NOT_FOUND_EXIT_STATUS)
      case e: NoClassDefFoundError =>
        logWarning(s"Failed to load $childMainClass: ${e.getMessage()}")
        if (e.getMessage.contains("org/apache/hadoop/hive")) {
          logInfo(s"Failed to load hive class.")
          logInfo("You need to build Spark with -Phive and -Phive-thriftserver.")
        }
        throw new SparkUserAppException(CLASS_NOT_FOUND_EXIT_STATUS)
    }

    // 通过classOf[]构建从属于mainClass的SparkApplication对象
    // 然后通过mainclass实例化了SparkApplication
    // SparkApplication是一个抽象类,这里主要是实现它的start()
    val app: SparkApplication = if (classOf[SparkApplication].isAssignableFrom(mainClass)) {
      mainClass.newInstance().asInstanceOf[SparkApplication]
    } else {
      // SPARK-4170
      if (classOf[scala.App].isAssignableFrom(mainClass)) {
        logWarning("Subclasses of scala.App may not work correctly. Use a main() method instead.")
      }
      // 如果mainclass无法实例化SparkApplication,则使用替代构建子类JavaMainApplication实例
      new JavaMainApplication(mainClass)
    }

    @tailrec
    def findCause(t: Throwable): Throwable = t match {
      case e: UndeclaredThrowableException =>
        if (e.getCause() != null) findCause(e.getCause()) else e
      case e: InvocationTargetException =>
        if (e.getCause() != null) findCause(e.getCause()) else e
      case e: Throwable =>
        e
    }

    try {
      // 启动实例 org.apache.spark.deploy.yarn.YarnClusterApplication
      app.start(childArgs.toArray, sparkConf)
    } catch {
      case t: Throwable =>
        throw findCause(t)
    }
  }

  /** Throw a SparkException with the given error message. */
  private def error(msg: String): Unit = throw new SparkException(msg)

}


/**
 * This entry point is used by the launcher library to start in-process Spark applications.
 */
private[spark] object InProcessSparkSubmit {

  def main(args: Array[String]): Unit = {
    val submit = new SparkSubmit()
    submit.doSubmit(args)
  }

}

object SparkSubmit extends CommandLineUtils with Logging {

  // Cluster managers
  private val YARN = 1
  private val STANDALONE = 2
  private val MESOS = 4
  private val LOCAL = 8
  private val KUBERNETES = 16
  private val ALL_CLUSTER_MGRS = YARN | STANDALONE | MESOS | LOCAL | KUBERNETES

  // Deploy modes
  private val CLIENT = 1
  private val CLUSTER = 2
  private val ALL_DEPLOY_MODES = CLIENT | CLUSTER

  // Special primary resource names that represent shells rather than application jars.
  private val SPARK_SHELL = "spark-shell"
  private val PYSPARK_SHELL = "pyspark-shell"
  private val SPARKR_SHELL = "sparkr-shell"
  private val SPARKR_PACKAGE_ARCHIVE = "sparkr.zip"
  private val R_PACKAGE_ARCHIVE = "rpkg.zip"

  private val CLASS_NOT_FOUND_EXIT_STATUS = 101

  // Following constants are visible for testing.
  private[deploy] val YARN_CLUSTER_SUBMIT_CLASS =
    "org.apache.spark.deploy.yarn.YarnClusterApplication"
  private[deploy] val REST_CLUSTER_SUBMIT_CLASS = classOf[RestSubmissionClientApp].getName()
  private[deploy] val STANDALONE_CLUSTER_SUBMIT_CLASS = classOf[ClientApp].getName()
  private[deploy] val KUBERNETES_CLUSTER_SUBMIT_CLASS =
    "org.apache.spark.deploy.k8s.submit.KubernetesClientApplication"

  override def main(args: Array[String]): Unit = {

//    创建SparkSubmit实例
    val submit = new SparkSubmit() {
      self =>
//      重写了class SparkSubmit的解析加载参数方法
      override protected def parseArguments(args: Array[String]): SparkSubmitArguments = {
        new SparkSubmitArguments(args) {
          override protected def logInfo(msg: => String): Unit = self.logInfo(msg)

          override protected def logWarning(msg: => String): Unit = self.logWarning(msg)
        }
      }
//      日志输出方法
      override protected def logInfo(msg: => String): Unit = printMessage(msg)
//      warning输出方法
      override protected def logWarning(msg: => String): Unit = printMessage(s"Warning: $msg")
//      重新任务提交方法
      override def doSubmit(args: Array[String]): Unit = {
        try {
          super.doSubmit(args)
        } catch {
          case e: SparkUserAppException =>
            exitFn(e.exitCode)
        }
      }

    }
    // 调用上面SparkSubmit实例的doSubmit()
    submit.doSubmit(args)
  }

  /**
   * Return whether the given primary resource represents a user jar.
   */
  private[deploy] def isUserJar(res: String): Boolean = {
    !isShell(res) && !isPython(res) && !isInternal(res) && !isR(res)
  }

  /**
   * Return whether the given primary resource represents a shell.
   */
  private[deploy] def isShell(res: String): Boolean = {
    (res == SPARK_SHELL || res == PYSPARK_SHELL || res == SPARKR_SHELL)
  }

  /**
   * Return whether the given main class represents a sql shell.
   */
  private[deploy] def isSqlShell(mainClass: String): Boolean = {
    mainClass == "org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver"
  }

  /**
   * Return whether the given main class represents a thrift server.
   */
  private def isThriftServer(mainClass: String): Boolean = {
    mainClass == "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2"
  }

  /**
   * Return whether the given primary resource requires running python.
   */
  private[deploy] def isPython(res: String): Boolean = {
    res != null && res.endsWith(".py") || res == PYSPARK_SHELL
  }

  /**
   * Return whether the given primary resource requires running R.
   */
  private[deploy] def isR(res: String): Boolean = {
    res != null && res.endsWith(".R") || res == SPARKR_SHELL
  }

  private[deploy] def isInternal(res: String): Boolean = {
    res == SparkLauncher.NO_RESOURCE
  }

}

/** Provides utility functions to be used inside SparkSubmit. */
private[spark] object SparkSubmitUtils {

  // Exposed for testing
  var printStream = SparkSubmit.printStream

  // Exposed for testing.
  // These components are used to make the default exclusion rules for Spark dependencies.
  // We need to specify each component explicitly, otherwise we miss spark-streaming-kafka-0-8 and
  // other spark-streaming utility components. Underscore is there to differentiate between
  // spark-streaming_2.1x and spark-streaming-kafka-0-8-assembly_2.1x
  val IVY_DEFAULT_EXCLUDES = Seq("catalyst_", "core_", "graphx_", "kvstore_", "launcher_", "mllib_",
    "mllib-local_", "network-common_", "network-shuffle_", "repl_", "sketch_", "sql_", "streaming_",
    "tags_", "unsafe_")

  /**
   * Represents a Maven Coordinate
   * @param groupId the groupId of the coordinate
   * @param artifactId the artifactId of the coordinate
   * @param version the version of the coordinate
   */
  private[deploy] case class MavenCoordinate(groupId: String, artifactId: String, version: String) {
    override def toString: String = s"$groupId:$artifactId:$version"
  }

  /**
   * Extracts maven coordinates from a comma-delimited string. Coordinates should be provided
   * in the format `groupId:artifactId:version` or `groupId/artifactId:version`.
   * @param coordinates Comma-delimited string of maven coordinates
   * @return Sequence of Maven coordinates
   */
  def extractMavenCoordinates(coordinates: String): Seq[MavenCoordinate] = {
    coordinates.split(",").map { p =>
      val splits = p.replace("/", ":").split(":")
      require(splits.length == 3, s"Provided Maven Coordinates must be in the form " +
        s"'groupId:artifactId:version'. The coordinate provided is: $p")
      require(splits(0) != null && splits(0).trim.nonEmpty, s"The groupId cannot be null or " +
        s"be whitespace. The groupId provided is: ${splits(0)}")
      require(splits(1) != null && splits(1).trim.nonEmpty, s"The artifactId cannot be null or " +
        s"be whitespace. The artifactId provided is: ${splits(1)}")
      require(splits(2) != null && splits(2).trim.nonEmpty, s"The version cannot be null or " +
        s"be whitespace. The version provided is: ${splits(2)}")
      new MavenCoordinate(splits(0), splits(1), splits(2))
    }
  }

  /** Path of the local Maven cache. */
  private[spark] def m2Path: File = {
    if (Utils.isTesting) {
      // test builds delete the maven cache, and this can cause flakiness
      new File("dummy", ".m2" + File.separator + "repository")
    } else {
      new File(System.getProperty("user.home"), ".m2" + File.separator + "repository")
    }
  }

  /**
   * Extracts maven coordinates from a comma-delimited string
   * @param defaultIvyUserDir The default user path for Ivy
   * @return A ChainResolver used by Ivy to search for and resolve dependencies.
   */
  def createRepoResolvers(defaultIvyUserDir: File): ChainResolver = {
    // We need a chain resolver if we want to check multiple repositories
    val cr = new ChainResolver
    cr.setName("spark-list")

    val localM2 = new IBiblioResolver
    localM2.setM2compatible(true)
    localM2.setRoot(m2Path.toURI.toString)
    localM2.setUsepoms(true)
    localM2.setName("local-m2-cache")
    cr.add(localM2)

    val localIvy = new FileSystemResolver
    val localIvyRoot = new File(defaultIvyUserDir, "local")
    localIvy.setLocal(true)
    localIvy.setRepository(new FileRepository(localIvyRoot))
    val ivyPattern = Seq(localIvyRoot.getAbsolutePath, "[organisation]", "[module]", "[revision]",
      "ivys", "ivy.xml").mkString(File.separator)
    localIvy.addIvyPattern(ivyPattern)
    val artifactPattern = Seq(localIvyRoot.getAbsolutePath, "[organisation]", "[module]",
      "[revision]", "[type]s", "[artifact](-[classifier]).[ext]").mkString(File.separator)
    localIvy.addArtifactPattern(artifactPattern)
    localIvy.setName("local-ivy-cache")
    cr.add(localIvy)

    // the biblio resolver resolves POM declared dependencies
    val br: IBiblioResolver = new IBiblioResolver
    br.setM2compatible(true)
    br.setUsepoms(true)
    br.setName("central")
    cr.add(br)

    val sp: IBiblioResolver = new IBiblioResolver
    sp.setM2compatible(true)
    sp.setUsepoms(true)
    sp.setRoot("https://dl.bintray.com/spark-packages/maven")
    sp.setName("spark-packages")
    cr.add(sp)
    cr
  }

  /**
   * Output a comma-delimited list of paths for the downloaded jars to be added to the classpath
   * (will append to jars in SparkSubmit).
   * @param artifacts Sequence of dependencies that were resolved and retrieved
   * @param cacheDirectory directory where jars are cached
   * @return a comma-delimited list of paths for the dependencies
   */
  def resolveDependencyPaths(
      artifacts: Array[AnyRef],
      cacheDirectory: File): String = {
    artifacts.map { artifactInfo =>
      val artifact = artifactInfo.asInstanceOf[Artifact].getModuleRevisionId
      cacheDirectory.getAbsolutePath + File.separator +
        s"${artifact.getOrganisation}_${artifact.getName}-${artifact.getRevision}.jar"
    }.mkString(",")
  }

  /** Adds the given maven coordinates to Ivy's module descriptor. */
  def addDependenciesToIvy(
      md: DefaultModuleDescriptor,
      artifacts: Seq[MavenCoordinate],
      ivyConfName: String): Unit = {
    artifacts.foreach { mvn =>
      val ri = ModuleRevisionId.newInstance(mvn.groupId, mvn.artifactId, mvn.version)
      val dd = new DefaultDependencyDescriptor(ri, false, false)
      dd.addDependencyConfiguration(ivyConfName, ivyConfName + "(runtime)")
      // scalastyle:off println
      printStream.println(s"${dd.getDependencyId} added as a dependency")
      // scalastyle:on println
      md.addDependency(dd)
    }
  }

  /** Add exclusion rules for dependencies already included in the spark-assembly */
  def addExclusionRules(
      ivySettings: IvySettings,
      ivyConfName: String,
      md: DefaultModuleDescriptor): Unit = {
    // Add scala exclusion rule
    md.addExcludeRule(createExclusion("*:scala-library:*", ivySettings, ivyConfName))

    IVY_DEFAULT_EXCLUDES.foreach { comp =>
      md.addExcludeRule(createExclusion(s"org.apache.spark:spark-$comp*:*", ivySettings,
        ivyConfName))
    }
  }

  /**
   * Build Ivy Settings using options with default resolvers
   * @param remoteRepos Comma-delimited string of remote repositories other than maven central
   * @param ivyPath The path to the local ivy repository
   * @return An IvySettings object
   */
  def buildIvySettings(remoteRepos: Option[String], ivyPath: Option[String]): IvySettings = {
    val ivySettings: IvySettings = new IvySettings
    processIvyPathArg(ivySettings, ivyPath)

    // create a pattern matcher
    ivySettings.addMatcher(new GlobPatternMatcher)
    // create the dependency resolvers
    val repoResolver = createRepoResolvers(ivySettings.getDefaultIvyUserDir)
    ivySettings.addResolver(repoResolver)
    ivySettings.setDefaultResolver(repoResolver.getName)
    processRemoteRepoArg(ivySettings, remoteRepos)
    ivySettings
  }

  /**
   * Load Ivy settings from a given filename, using supplied resolvers
   * @param settingsFile Path to Ivy settings file
   * @param remoteRepos Comma-delimited string of remote repositories other than maven central
   * @param ivyPath The path to the local ivy repository
   * @return An IvySettings object
   */
  def loadIvySettings(
      settingsFile: String,
      remoteRepos: Option[String],
      ivyPath: Option[String]): IvySettings = {
    val file = new File(settingsFile)
    require(file.exists(), s"Ivy settings file $file does not exist")
    require(file.isFile(), s"Ivy settings file $file is not a normal file")
    val ivySettings: IvySettings = new IvySettings
    try {
      ivySettings.load(file)
    } catch {
      case e @ (_: IOException | _: ParseException) =>
        throw new SparkException(s"Failed when loading Ivy settings from $settingsFile", e)
    }
    processIvyPathArg(ivySettings, ivyPath)
    processRemoteRepoArg(ivySettings, remoteRepos)
    ivySettings
  }

  /* Set ivy settings for location of cache, if option is supplied */
  private def processIvyPathArg(ivySettings: IvySettings, ivyPath: Option[String]): Unit = {
    ivyPath.filterNot(_.trim.isEmpty).foreach { alternateIvyDir =>
      ivySettings.setDefaultIvyUserDir(new File(alternateIvyDir))
      ivySettings.setDefaultCache(new File(alternateIvyDir, "cache"))
    }
  }

  /* Add any optional additional remote repositories */
  private def processRemoteRepoArg(ivySettings: IvySettings, remoteRepos: Option[String]): Unit = {
    remoteRepos.filterNot(_.trim.isEmpty).map(_.split(",")).foreach { repositoryList =>
      val cr = new ChainResolver
      cr.setName("user-list")

      // add current default resolver, if any
      Option(ivySettings.getDefaultResolver).foreach(cr.add)

      // add additional repositories, last resolution in chain takes precedence
      repositoryList.zipWithIndex.foreach { case (repo, i) =>
        val brr: IBiblioResolver = new IBiblioResolver
        brr.setM2compatible(true)
        brr.setUsepoms(true)
        brr.setRoot(repo)
        brr.setName(s"repo-${i + 1}")
        cr.add(brr)
        // scalastyle:off println
        printStream.println(s"$repo added as a remote repository with the name: ${brr.getName}")
        // scalastyle:on println
      }

      ivySettings.addResolver(cr)
      ivySettings.setDefaultResolver(cr.getName)
    }
  }

  /** A nice function to use in tests as well. Values are dummy strings. */
  def getModuleDescriptor: DefaultModuleDescriptor = DefaultModuleDescriptor.newDefaultInstance(
    // Include UUID in module name, so multiple clients resolving maven coordinate at the same time
    // do not modify the same resolution file concurrently.
    ModuleRevisionId.newInstance("org.apache.spark",
      s"spark-submit-parent-${UUID.randomUUID.toString}",
      "1.0"))

  /**
   * Clear ivy resolution from current launch. The resolution file is usually at
   * ~/.ivy2/org.apache.spark-spark-submit-parent-$UUID-default.xml,
   * ~/.ivy2/resolved-org.apache.spark-spark-submit-parent-$UUID-1.0.xml, and
   * ~/.ivy2/resolved-org.apache.spark-spark-submit-parent-$UUID-1.0.properties.
   * Since each launch will have its own resolution files created, delete them after
   * each resolution to prevent accumulation of these files in the ivy cache dir.
   */
  private def clearIvyResolutionFiles(
      mdId: ModuleRevisionId,
      ivySettings: IvySettings,
      ivyConfName: String): Unit = {
    val currentResolutionFiles = Seq(
      s"${mdId.getOrganisation}-${mdId.getName}-$ivyConfName.xml",
      s"resolved-${mdId.getOrganisation}-${mdId.getName}-${mdId.getRevision}.xml",
      s"resolved-${mdId.getOrganisation}-${mdId.getName}-${mdId.getRevision}.properties"
    )
    currentResolutionFiles.foreach { filename =>
      new File(ivySettings.getDefaultCache, filename).delete()
    }
  }

  /**
   * Resolves any dependencies that were supplied through maven coordinates
   * @param coordinates Comma-delimited string of maven coordinates
   * @param ivySettings An IvySettings containing resolvers to use
   * @param exclusions Exclusions to apply when resolving transitive dependencies
   * @return The comma-delimited path to the jars of the given maven artifacts including their
   *         transitive dependencies
   */
  def resolveMavenCoordinates(
      coordinates: String,
      ivySettings: IvySettings,
      exclusions: Seq[String] = Nil,
      isTest: Boolean = false): String = {
    if (coordinates == null || coordinates.trim.isEmpty) {
      ""
    } else {
      val sysOut = System.out
      try {
        // To prevent ivy from logging to system out
        System.setOut(printStream)
        val artifacts = extractMavenCoordinates(coordinates)
        // Directories for caching downloads through ivy and storing the jars when maven coordinates
        // are supplied to spark-submit
        val packagesDirectory: File = new File(ivySettings.getDefaultIvyUserDir, "jars")
        // scalastyle:off println
        printStream.println(
          s"Ivy Default Cache set to: ${ivySettings.getDefaultCache.getAbsolutePath}")
        printStream.println(s"The jars for the packages stored in: $packagesDirectory")
        // scalastyle:on println

        val ivy = Ivy.newInstance(ivySettings)
        // Set resolve options to download transitive dependencies as well
        val resolveOptions = new ResolveOptions
        resolveOptions.setTransitive(true)
        val retrieveOptions = new RetrieveOptions
        // Turn downloading and logging off for testing
        if (isTest) {
          resolveOptions.setDownload(false)
          resolveOptions.setLog(LogOptions.LOG_QUIET)
          retrieveOptions.setLog(LogOptions.LOG_QUIET)
        } else {
          resolveOptions.setDownload(true)
        }

        // Default configuration name for ivy
        val ivyConfName = "default"

        // A Module descriptor must be specified. Entries are dummy strings
        val md = getModuleDescriptor

        md.setDefaultConf(ivyConfName)

        // Add exclusion rules for Spark and Scala Library
        addExclusionRules(ivySettings, ivyConfName, md)
        // add all supplied maven artifacts as dependencies
        addDependenciesToIvy(md, artifacts, ivyConfName)
        exclusions.foreach { e =>
          md.addExcludeRule(createExclusion(e + ":*", ivySettings, ivyConfName))
        }
        // resolve dependencies
        val rr: ResolveReport = ivy.resolve(md, resolveOptions)
        if (rr.hasError) {
          throw new RuntimeException(rr.getAllProblemMessages.toString)
        }
        // retrieve all resolved dependencies
        ivy.retrieve(rr.getModuleDescriptor.getModuleRevisionId,
          packagesDirectory.getAbsolutePath + File.separator +
            "[organization]_[artifact]-[revision](-[classifier]).[ext]",
          retrieveOptions.setConfs(Array(ivyConfName)))
        val paths = resolveDependencyPaths(rr.getArtifacts.toArray, packagesDirectory)
        val mdId = md.getModuleRevisionId
        clearIvyResolutionFiles(mdId, ivySettings, ivyConfName)
        paths
      } finally {
        System.setOut(sysOut)
      }
    }
  }

  private[deploy] def createExclusion(
      coords: String,
      ivySettings: IvySettings,
      ivyConfName: String): ExcludeRule = {
    val c = extractMavenCoordinates(coords)(0)
    val id = new ArtifactId(new ModuleId(c.groupId, c.artifactId), "*", "*", "*")
    val rule = new DefaultExcludeRule(id, ivySettings.getMatcher("glob"), null)
    rule.addConfiguration(ivyConfName)
    rule
  }

  def parseSparkConfProperty(pair: String): (String, String) = {
    pair.split("=", 2).toSeq match {
      case Seq(k, v) => (k, v)
      case _ => throw new SparkException(s"Spark config without '=': $pair")
    }
  }

}

/**
 * Provides an indirection layer for passing arguments as system properties or flags to
 * the user's driver program or to downstream launcher tools.
 */
private case class OptionAssigner(
    value: String,
    clusterManager: Int,
    deployMode: Int,
    clOption: String = null,
    confKey: String = null)
