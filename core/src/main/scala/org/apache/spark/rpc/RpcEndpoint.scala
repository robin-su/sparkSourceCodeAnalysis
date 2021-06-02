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

import org.apache.spark.SparkException

/**
 * A factory class to create the [[RpcEnv]]. It must have an empty constructor so that it can be
 * created using Reflection.
 */
private[spark] trait RpcEnvFactory {

  def create(config: RpcEnvConfig): RpcEnv
}

/**
 * An end point for the RPC that defines what functions to trigger given a message.
 *
 * It is guaranteed that `onStart`, `receive` and `onStop` will be called in sequence.
 *
 * The life-cycle of an endpoint is:
 *
 * {@code constructor -> onStart -> receive* -> onStop}
 *
 * Note: `receive` can be called concurrently. If you want `receive` to be thread-safe, please use
 * [[ThreadSafeRpcEndpoint]]
 *
 * If any error is thrown from one of [[RpcEndpoint]] methods except `onError`, `onError` will be
 * invoked with the cause. If `onError` throws an error, [[RpcEnv]] will ignore it.
 */
private[spark] trait RpcEndpoint {

  /**
   * RpcEndpoint 注册的那个 RpcEnv 对象
   * The [[RpcEnv]] that this [[RpcEndpoint]] is registered to.
   */
  val rpcEnv: RpcEnv

  /**
   * RpcEndpoint 对应的 RpcEndpointRef。onStart 方法被调用的时候，RpcEndpointRef有效，onStop 调用后，self会是null，
   * 注意由于在onStart之前，RpcEndpoint 还没有被注册，还没有有效的RpcEndpointRef，所以不要在onStart之前调用 self 方法
   *
   * The [[RpcEndpointRef]] of this [[RpcEndpoint]]. `self` will become valid when `onStart` is
   * called. And `self` will become `null` when `onStop` is called.
   *
   * Note: Because before `onStart`, [[RpcEndpoint]] has not yet been registered and there is not
   * valid [[RpcEndpointRef]] for it. So don't call `self` before `onStart` is called.
   */
  final def self: RpcEndpointRef = {
    require(rpcEnv != null, "rpcEnv has not been initialized")
    rpcEnv.endpointRef(this)
  }

  /**
   * 处理从RpcEndpointRef.send 或 RpcCallContext.reply 过来的消息，如果接收到一个未匹配的消息，
   * 会抛出 SparkException 并且发送给onError 方法
   *
   * Process messages from `RpcEndpointRef.send` or `RpcCallContext.reply`. If receiving a
   * unmatched message, `SparkException` will be thrown and sent to `onError`.
   */
  def receive: PartialFunction[Any, Unit] = {
    case _ => throw new SparkException(self + " does not implement 'receive'")
  }

  /**
   * 处理从RpcEndpointRef.ask发过来的消息，如果接收到一个未匹配的消息，会抛出 SparkException 并且发送给onError 方法
   * Process messages from `RpcEndpointRef.ask`. If receiving a unmatched message,
   * `SparkException` will be thrown and sent to `onError`.
   */
  def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case _ => context.sendFailure(new SparkException(self + " won't reply anything"))
  }

  /**
   * 在消息处理过程中，如果有异常都会调用此方法
   * Invoked when any exception is thrown during handling messages.
   */
  def onError(cause: Throwable): Unit = {
    // By default, throw e and let RpcEnv handle it
    throw cause
  }

  /**
   * 当remoteAddress 连接上当前节点时被调用
   * Invoked when `remoteAddress` is connected to the current node.
   */
  def onConnected(remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
   * 当当前节点丢失掉 remoteAddress 后被调用
   * Invoked when `remoteAddress` is lost.
   */
  def onDisconnected(remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
   * 当连接当前节点和remoteAddress时，有网络错误发生时被调用
   * Invoked when some network error happens in the connection between the current node and
   * `remoteAddress`.
   */
  def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
   * 在RpcEndpoint开始处理其他消息之前被调用
   * Invoked before [[RpcEndpoint]] starts to handle any message.
   */
  def onStart(): Unit = {
    // By default, do nothing.
  }

  /**
   * 当RpcEndpoint停止时被调用，self 将会是null，不能用于发送消息
   * Invoked when [[RpcEndpoint]] is stopping. `self` will be `null` in this method and you cannot
   * use it to send or ask messages.
   */
  def onStop(): Unit = {
    // By default, do nothing.
  }

  /**
   * 停止RpcEndpoint
   * A convenient method to stop [[RpcEndpoint]].
   */
  final def stop(): Unit = {
    val _self = self
    if (_self != null) {
      rpcEnv.stop(_self)
    }
  }
}

/**
 * A trait that requires RpcEnv thread-safely sending messages to it.
 * 需要RpcEnv线程安全地向其发送消息的trait。线程安全意味着在通过相同的ThreadSafeRpcEndpoint
 * 处理一条消息完成后再处理下一个消息。换句话说，在处理下一条消息时，可以看到对ThreadSafeRpcEndpoint的内部字段的更改，
 * 并且ThreadSafeRpcEndpoint中的字段不需要是volatile或等效的。但是，
 * 不能保证同一个线程将为不同的消息执行相同的ThreadSafeRpcEndpoint。
   即顺序处理消息，不能同时并发处理。
 * Thread-safety means processing of one message happens before processing of the next message by
 * the same [[ThreadSafeRpcEndpoint]]. In the other words, changes to internal fields of a
 * [[ThreadSafeRpcEndpoint]] are visible when processing the next message, and fields in the
 * [[ThreadSafeRpcEndpoint]] need not be volatile or equivalent.
 *
 * However, there is no guarantee that the same thread will be executing the same
 * [[ThreadSafeRpcEndpoint]] for different messages.
 */
private[spark] trait ThreadSafeRpcEndpoint extends RpcEndpoint
