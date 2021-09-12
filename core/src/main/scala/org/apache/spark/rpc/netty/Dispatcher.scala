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

package org.apache.spark.rpc.netty

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.concurrent.Promise
import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.network.client.RpcResponseCallback
import org.apache.spark.rpc._
import org.apache.spark.util.ThreadUtils

/**
 * Dispatcher 是消息的分发器，负责将消息分发给适合的 endpoint
   抽象出来 Inbox 的原因在于，Diapatcher 的职责变得单一，只需要把数据分发就可以了。
   具体分发数据要如何处理的问题留给了 Inbox，Inbox 把关注点放在了 如何处理这些消息上。
   考虑并解决了 一次性批量处理消息问题、多线程安全问题、异常抛出问题，多消息分支处理问题等等问题。
 * A message dispatcher, responsible for routing RPC messages to the appropriate endpoint(s).
 *
 * @param numUsableCores Number of CPU cores allocated to the process, for sizing the thread pool.
 *                       If 0, will consider the available CPUs on the host.
 */
private[netty] class Dispatcher(nettyEnv: NettyRpcEnv, numUsableCores: Int) extends Logging {

  private class EndpointData(
      val name: String,
      val endpoint: RpcEndpoint,
      val ref: NettyRpcEndpointRef) {
    val inbox = new Inbox(ref, endpoint)
  }

  /* 端点实例名称与端点数据EndpointData之间映射关系的缓存。有了这个缓存，就可以使用端点名称从中快速获取或删除EndpointData了 */
  private val endpoints: ConcurrentMap[String, EndpointData] =
    new ConcurrentHashMap[String, EndpointData]
  /*  端点实例RpcEndpoint与端点实例引用RpcEndpointRef之间映射关系的缓存。有了这个缓存，就可以使用端点实例从中快速获取或删除端点实例引用了。 */
  private val endpointRefs: ConcurrentMap[RpcEndpoint, RpcEndpointRef] =
    new ConcurrentHashMap[RpcEndpoint, RpcEndpointRef]

  /**
   * 存储端点数据EndpointData的阻塞队列。只有Inbox中有消息的EndpointData才会被放入此阻塞队列。
   *
   * receivers 是一个 LinkedBlockingQueue[EndpointData] 消息阻塞队列，用于存放 EndpointData 对象。它主要用于追踪
   * 那些可能会包含需要处理消息receiver（即EndpointData）。在post消息到Dispatcher时，一般会先post 到 EndpointData 的 Inbox 中，
   * 然后，再将 EndpointData对象放入 receivers 中
   */
  // Track the receivers whose inboxes may contain messages.
  private val receivers = new LinkedBlockingQueue[EndpointData]

  /**
   * Dispatcher是否停止的状态。
   * True if the dispatcher has been stopped. Once stopped, all messages posted will be bounced
   * immediately.
   * stopped 标志 Dispatcher 是否已经停止了
   */
  @GuardedBy("this")
  private var stopped = false

  /**
   * 注册主要做三件事：
   * 1：endpoints 中添加EndpointData
   * 2：添加endpointRefs信息
   * 3: 向receivers队列中添加EndpointData消息
   *
   * @param name
   * @param endpoint
   * @return
   */
  def registerRpcEndpoint(name: String, endpoint: RpcEndpoint): NettyRpcEndpointRef = {
    // 初始化RpcEndpointAddress
    val addr = RpcEndpointAddress(nettyEnv.address, name)
    // 使用Netty
    val endpointRef = new NettyRpcEndpointRef(nettyEnv.conf, addr, nettyEnv)
    synchronized {
      // 若Dispatcher已经停止，则抛出非法状态异常
      if (stopped) {
        throw new IllegalStateException("RpcEnv has been stopped")
      }
      // 若已经被注册，则不能从新注册。有返回值说明已经被注册，所以这个时候不能再次注册。
      if (endpoints.putIfAbsent(name, new EndpointData(name, endpoint, endpointRef)) != null) {
        throw new IllegalArgumentException(s"zh is already an RpcEndpoint called $name")
      }
      // 从endpoints中获取EndpointData
      val data = endpoints.get(name)
      // 注册成：RpcEndpoint 和 RpcEndpointRef 的映射关系
      endpointRefs.put(data.endpoint, data.ref)
      // 向队列中添加数据，若队列已经满了，则返回false
      receivers.offer(data)  // for the OnStart message
    }
    endpointRef
  }

  def getRpcEndpointRef(endpoint: RpcEndpoint): RpcEndpointRef = endpointRefs.get(endpoint)

  def removeRpcEndpointRef(endpoint: RpcEndpoint): Unit = endpointRefs.remove(endpoint)

  // Should be idempotent
  private def unregisterRpcEndpoint(name: String): Unit = {
    // 1.移除EndpointData
    val data = endpoints.remove(name)
    if (data != null) {
      // 停止inbox
      data.inbox.stop()
      // 放入OnStop
      receivers.offer(data)  // for the OnStop message
    }
    // Don't clean `endpointRefs` here because it's possible that some messages are being processed
    // now and they can use `getRpcEndpointRef`. So `endpointRefs` will be cleaned in Inbox via
    // `removeRpcEndpointRef`.
  }

  def stop(rpcEndpointRef: RpcEndpointRef): Unit = {
    synchronized {
      if (stopped) {
        // This endpoint will be stopped by Dispatcher.stop() method.
        return
      }
      unregisterRpcEndpoint(rpcEndpointRef.name)
    }
  }

  /**
   * Send a message to all registered [[RpcEndpoint]]s in this process.
   *
   * This can be used to make network events known to all end points (e.g. "a new node connected").
   */
  def postToAll(message: InboxMessage): Unit = {
    // 取出endpoints的key迭代器
    val iter = endpoints.keySet().iterator()
    while (iter.hasNext) {
      val name = iter.next
        postMessage(name, message, (e) => { e match {
          case e: RpcEnvStoppedException => logDebug (s"Message $message dropped. ${e.getMessage}")
          case e: Throwable => logWarning(s"Message $message dropped. ${e.getMessage}")
        }}
      )}
  }

  /** Posts a message sent by a remote endpoint. */
  def postRemoteMessage(message: RequestMessage, callback: RpcResponseCallback): Unit = {
    val rpcCallContext =
      new RemoteNettyRpcCallContext(nettyEnv, callback, message.senderAddress)
    val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
    postMessage(message.receiver.name, rpcMessage, (e) => callback.onFailure(e))
  }

  /** Posts a message sent by a local endpoint. */
  def postLocalMessage(message: RequestMessage, p: Promise[Any]): Unit = {
    val rpcCallContext =
      new LocalNettyRpcCallContext(message.senderAddress, p)
    val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
    postMessage(message.receiver.name, rpcMessage, (e) => p.tryFailure(e))
  }

  /** Posts a one-way message. */
  def postOneWayMessage(message: RequestMessage): Unit = {
    postMessage(message.receiver.name, OneWayMessage(message.senderAddress, message.content),
      (e) => throw e)
  }

  /**
   * 将消息发布到特殊的endpoint
   * Posts a message to a specific endpoint.
   *
   * @param endpointName name of the endpoint.
   * @param message the message to post
   * @param callbackIfStopped callback function if the endpoint is stopped.
   */
  private def postMessage(
      endpointName: String,
      message: InboxMessage,
      callbackIfStopped: (Exception) => Unit): Unit = {
    val error = synchronized {
      // 根据endpointName从映射中获取EndpointData
      val data = endpoints.get(endpointName)
      if (stopped) {
        // 若Dispatcher已经停止，则抛出RpcEnvStoppedException
        Some(new RpcEnvStoppedException())
      } else if (data == null) {
        // 若取出的Endpoint是空的，则抛出SparkException异常
        Some(new SparkException(s"Could not find $endpointName."))
      } else {
        //2.将消费的消息发送到inbox中
        data.inbox.post(message)
        //3.将data放到待消费的receiver中
        receivers.offer(data)
        None
      }
    }
    // We don't need to call `onStop` in the `synchronized` block
    error.foreach(callbackIfStopped)
  }

  def stop(): Unit = {
    synchronized {
      if (stopped) {
        return
      }
      stopped = true
    }
    // Stop all endpoints. This will queue all endpoints for processing by the message loops.
    endpoints.keySet().asScala.foreach(unregisterRpcEndpoint)
    // Enqueue a message that tells the message loops to stop.
    receivers.offer(PoisonPill)
    threadpool.shutdown()
  }

  def awaitTermination(): Unit = {
    threadpool.awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)
  }

  /**
   * Return if the endpoint exists
   */
  def verify(name: String): Boolean = {
    endpoints.containsKey(name)
  }

  /** Thread pool used for dispatching messages. */
  private val threadpool: ThreadPoolExecutor = {
    // 计算线程池可以使用的核心数
    val availableCores =
      if (numUsableCores > 0) numUsableCores else Runtime.getRuntime.availableProcessors()
    val numThreads = nettyEnv.conf.getInt("spark.rpc.netty.dispatcher.numThreads",
      math.max(2, availableCores))
    // 初始化线程池
    val pool = ThreadUtils.newDaemonFixedThreadPool(numThreads, "dispatcher-event-loop")
    for (i <- 0 until numThreads) {
      // 初始化线程池之后，使用该线程池来执行MessageLoop任务，MessageLoop是一个Runnable对象。它会不停的从receiver
      // 队列中，把放入的EndpointData对象取出来，并且调用其inbox成员变量的process方法
      pool.execute(new MessageLoop)
    }
    pool
  }

  /** Message loop used for dispatching messages. */
  private class MessageLoop extends Runnable {
    override def run(): Unit = {
      try {
        while (true) {
          try {
            /**
             * 由于receivers是个阻塞队列，所以当receivers中没有EndpointData时，MessageLoop线程会被阻塞。
             */
            // 从receivers对象中获取EndpointData数据
            val data = receivers.take()
            // 若取到对象是空的EndpointData
            if (data == PoisonPill) {
              /**
               * 如果取到的EndpointData是“毒药”（PoisonPill），那么此MessageLoop线程将退出（通过return语句）。
               * 这里有个动作就是将PoisonPill重新放入到receivers中，这是因为threadpool线程池极有可能不止一个MessageLoop线程，
               * 为了让大家都“毒发身亡”，还需要把“毒药”放回到receivers中，这样其他“活着”的线程就会再次误食“毒药”，
               * 达到所有MessageLoop线程都结束的效果。
               */
              // 将PoisonPill对象喂给receivers吃，当threadpool执行MessageLoop任务时，取到PoisonPill，马上退出
              // 线程也就死掉了。PoisonPill命名很形象，关闭线程池的方式也是优雅的，是值得我们在工作中去学习和应用的。
              // Put PoisonPill back so that other MessageLoops can see it.
              receivers.offer(PoisonPill)
              return // 线程马上退出，此时线程也就死掉了
            }
            // 调用data的inbox属性的process方法进行处理
            data.inbox.process(Dispatcher.this)
          } catch {
            case NonFatal(e) => logError(e.getMessage, e)
          }
        }
      } catch {
        case _: InterruptedException => // exit
        case t: Throwable =>
          try {
            // Re-submit a MessageLoop so that Dispatcher will still work if
            // UncaughtExceptionHandler decides to not kill JVM.
            threadpool.execute(new MessageLoop)
          } finally {
            throw t
          }
      }
    }
  }

  /** A poison endpoint that indicates MessageLoop should exit its message loop. */
  private val PoisonPill = new EndpointData(null, null, null)
}
