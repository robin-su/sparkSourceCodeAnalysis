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

package org.apache.spark.launcher

import java.net.{InetAddress, Socket}

import org.apache.spark.{SPARK_VERSION, SparkConf}
import org.apache.spark.launcher.LauncherProtocol._
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * 当Spark应用程序没有在用户应用程序中运行，而是运行在单独的进程中时，用户可以在用户应用程序中使用LauncherServer与Spark应用程序通信。
 * LauncherServer将提供Socket连接的服务端，与Spark应用程序中的Socket连接的客户端通信。
 *
 *
 * A class that can be used to talk to a launcher server. Users should extend this class to
 * provide implementation for the abstract methods.
 *
 * See `LauncherServer` for an explanation of how launcher communication works.
 */
private[spark] abstract class LauncherBackend {

  /**
   * 读取与LauncherServer建立的Socket连接上的消息的线程。
   */
  private var clientThread: Thread = _
  /**
   * 即BackendConnection实例。
   */
  private var connection: BackendConnection = _
  /**
   * LauncherBackend的最后一次状态。lastState的类型是枚举类型SparkAppHandle.State。
   * SparkAppHandle.State共有未知（UNKNOWN）、已连接（CONNECTED）、已提交（SUBMITTED）、
   * 运行中（RUNNING）、已完成（FINISHED）、已失败（FAILED）、已被杀（KILLED）、丢失（LOST）等状态。
   */
  private var lastState: SparkAppHandle.State = _
  /**
   * clientThread是否与LauncherServer已经建立了Socket连接的状态。
   */
  @volatile private var _isConnected = false

  protected def conf: SparkConf

  /**
   * 调用该方法构建BackendConnection
   */
  def connect(): Unit = {
    /**
     * LauncherServer的Socket端口，通过spark.launcher.port，或环境变量中_SPARK_LAUNCHER_PORT的配置获取
     */
    val port = conf.getOption(LauncherProtocol.CONF_LAUNCHER_PORT)
      .orElse(sys.env.get(LauncherProtocol.ENV_LAUNCHER_PORT))
      .map(_.toInt)
    /**
     * 密钥，通过spark.launcher.secret或者
     */
    val secret = conf.getOption(LauncherProtocol.CONF_LAUNCHER_SECRET)
      .orElse(sys.env.get(LauncherProtocol.ENV_LAUNCHER_SECRET))
    if (port != None && secret != None) {
      val s = new Socket(InetAddress.getLoopbackAddress(), port.get)
      connection = new BackendConnection(s)
      connection.send(new Hello(secret.get, SPARK_VERSION))
      clientThread = LauncherBackend.threadFactory.newThread(connection)
      clientThread.start()
      _isConnected = true
    }
  }

  def close(): Unit = {
    if (connection != null) {
      try {
        connection.close()
      } finally {
        if (clientThread != null) {
          clientThread.join()
        }
      }
    }
  }

  /**
   * 用于向LauncherServer发送SetAppId消息
   * @param appId
   */
  def setAppId(appId: String): Unit = {
    if (connection != null && isConnected) {
      connection.send(new SetAppId(appId))
    }
  }

  /**
   * 用于向LauncherServer发送SetState消息
   *
   * @param state
   */
  def setState(state: SparkAppHandle.State): Unit = {
    if (connection != null && isConnected && lastState != state) {
      connection.send(new SetState(state))
      lastState = state
    }
  }

  /**
   * isConnected方法返回clientThread是否与LauncherServer已经建立了Socket连接的状态。
   */
  /** Return whether the launcher handle is still connected to this backend. */
  def isConnected(): Boolean = _isConnected

  /**
   * Implementations should provide this method, which should try to stop the application
   * as gracefully as possible.
   */
  protected def onStopRequest(): Unit

  /**
   * Callback for when the launcher handle disconnects from this backend.
   */
  protected def onDisconnected() : Unit = { }

  private def fireStopRequest(): Unit = {
    val thread = LauncherBackend.threadFactory.newThread(new Runnable() {
      override def run(): Unit = Utils.tryLogNonFatalError {
        onStopRequest()
      }
    })
    thread.start()
  }

  /**
   * 构造BackendConnection的过程中，BackendConnection会和LauncherServer之间建立起Socket连接。
   * BackendConnection（实现了java.lang.Runnable接口）将不断从Socket连接中读取LauncherServer发送的数据。
   *
   * @param s
   */
  private class BackendConnection(s: Socket) extends LauncherConnection(s) {

    override protected def handle(m: Message): Unit = m match {
      case _: Stop =>
        fireStopRequest()

      case _ =>
        throw new IllegalArgumentException(s"Unexpected message type: ${m.getClass().getName()}")
    }

    override def close(): Unit = {
      try {
        _isConnected = false
        super.close()
      } finally {
        onDisconnected()
      }
    }

  }

}

private object LauncherBackend {

  val threadFactory = ThreadUtils.namedThreadFactory("LauncherBackend")

}
