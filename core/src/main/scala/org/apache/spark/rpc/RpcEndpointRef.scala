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

package org.apache.spark.rpc

import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.util.RpcUtils

/**
 * RpcEndPointRef：远程的RpcEndpoint引用，RpcEndpointRef是线程安全的。
 * A reference for a remote [[RpcEndpoint]]. [[RpcEndpointRef]] is thread-safe.
 */
private[spark] abstract class RpcEndpointRef(conf: SparkConf)
  extends Serializable with Logging {

  // 最大尝试连接次数。可以通过 spark.rpc.numRetries 参数来指定，默认是 3 次。 该变量暂时没有使用。
  private[this] val maxRetries = RpcUtils.numRetries(conf)
  // 每次尝试连接最大等待毫秒值。可以通过 spark.rpc.retry.wait 参数来指定，默认是 3s。该变量暂时没有使用。
  private[this] val retryWaitMs = RpcUtils.retryWaitMs(conf)
  // spark 默认 ask 请求操作超时时间。 可以通过 spark.rpc.askTimeout 或 spark.network.timeout参数来指定，默认是120s。
  private[this] val defaultAskTimeout = RpcUtils.askRpcTimeout(conf)

  /**
   * 抽象方法，返回 RpcEndpointRef的RpcAddress
   * return the address for the [[RpcEndpointRef]]
   */
  def address: RpcAddress

  // 抽象方法，返回 endpoint 的name
  def name: String

  /**
   * 发送单向异步的消息。所谓“单向”就是发送完后就会忘记此次发送，不会有任何状态要记录，
   * 也不会期望得到服务端的回复。send采用了at-most-once的投递规则
   *
   * 抽象方法，发送单向的异步消息，满足 即发即忘 语义。
   * Sends a one-way asynchronous message. Fire-and-forget semantics.
   */
  def send(message: Any): Unit

  /**
   * 抽象方法。发送消息到相应的 RpcEndpoint.receiveAndReply , 并返回 Future 以在默认超时内接收返回值。
   * 它有两个重载方法：其中没有RpcTimeOut 的ask方法添加一个 defaultAskTimeout 参数继续调用 有RpcTimeOut 的ask方法。
   *
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a [[Future]] to
   * receive the reply within the specified timeout.
   *
   * This method only sends the message once and never retries.
   */
  def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T]

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a [[Future]] to
   * receive the reply within a default timeout.
   *
   * This method only sends the message once and never retries.
   */
  def ask[T: ClassTag](message: Any): Future[T] = ask(message, defaultAskTimeout)

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply]] and get its result within a
   * default timeout, throw an exception if this fails.
   *
   * Note: this is a blocking action which may cost a lot of time,  so don't call it in a message
   * loop of [[RpcEndpoint]].

   * @param message the message to send
   * @tparam T type of the reply message
   * @return the reply message from the corresponding [[RpcEndpoint]]
   */
  def askSync[T: ClassTag](message: Any): T = askSync(message, defaultAskTimeout)

  /**
   * 调用抽象方法ask。跟ask类似，有两个重载方法：其中没有RpcTimeOut 的askSync方法添加一个 defaultAskTimeout 参数继续调用
   * 有RpcTimeOut 的askSync方法。有RpcTimeOut 的askSync方法 会调用 ask 方法生成一个Future 对象，然后等待任务执行完毕后返回。
   * 注意，这里面其实就涉及到了模板方法模式。ask跟askSync都是设定好了，ask 要返回一个Future 对象，askSync则是 调用 ask 返回的Future 对象，
     然后等待 future 的 result 方法返回。
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply]] and get its result within a
   * specified timeout, throw an exception if this fails.
   *
   * Note: this is a blocking action which may cost a lot of time, so don't call it in a message
   * loop of [[RpcEndpoint]].
   *
   * @param message the message to send
   * @param timeout the timeout duration
   * @tparam T type of the reply message
   * @return the reply message from the corresponding [[RpcEndpoint]]
   */
  def askSync[T: ClassTag](message: Any, timeout: RpcTimeout): T = {
    val future = ask[T](message, timeout)
    timeout.awaitResult(future)
  }

}
