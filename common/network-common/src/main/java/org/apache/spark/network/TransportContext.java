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

package org.apache.spark.network;

import java.util.ArrayList;
import java.util.List;

import io.netty.channel.Channel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.client.TransportResponseHandler;
import org.apache.spark.network.protocol.MessageDecoder;
import org.apache.spark.network.protocol.MessageEncoder;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.TransportChannelHandler;
import org.apache.spark.network.server.TransportRequestHandler;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.util.TransportFrameDecoder;

/**
 * 首先这个上下文对象是一个创建TransportServer, TransportClientFactory，使用TransportChannelHandler建立
 * netty channel pipeline的上下文，这也是它的三个主要功能。TransportClient 提供了两种通信协议：控制层面的RPC
 * 以及数据层面的 "chunk抓取"。用户通过构造方法传入的 rpcHandler 负责处理RPC 请求。并且 rpcHandler 负责设置流，
 * 这些流可以使用零拷贝IO以数据块的形式流式传输。
 *
 * TransportServer 和 TransportClientFactory 都为每一个channel创建一个 TransportChannelHandler对象。
 * 每一个TransportChannelHandler 包含一个 TransportClient，这使服务器进程能够在现有通道上将消息发送回客户端。
 * Contains the context to create a {@link TransportServer}, {@link TransportClientFactory}, and to
 * setup Netty Channel pipelines with a
 * {@link org.apache.spark.network.server.TransportChannelHandler}.
 *
 * There are two communication protocols that the TransportClient provides, control-plane RPCs and
 * data-plane "chunk fetching". The handling of the RPCs is performed outside of the scope of the
 * TransportContext (i.e., by a user-provided handler), and it is responsible for setting up streams
 * which can be streamed through the data plane in chunks using zero-copy IO.
 *
 * The TransportServer and TransportClientFactory both create a TransportChannelHandler for each
 * channel. As each TransportChannelHandler contains a TransportClient, this enables server
 * processes to send messages back to the client on an existing channel.
 */
public class TransportContext {
  private static final Logger logger = LoggerFactory.getLogger(TransportContext.class);

  private final TransportConf conf;
  private final RpcHandler rpcHandler;
  private final boolean closeIdleConnections;

  /**
   * Force to create MessageEncoder and MessageDecoder so that we can make sure they will be created
   * before switching the current context class loader to ExecutorClassLoader.
   *
   * Netty's MessageToMessageEncoder uses Javassist to generate a matcher class and the
   * implementation calls "Class.forName" to check if this calls is already generated. If the
   * following two objects are created in "ExecutorClassLoader.findClass", it will cause
   * "ClassCircularityError". This is because loading this Netty generated class will call
   * "ExecutorClassLoader.findClass" to search this class, and "ExecutorClassLoader" will try to use
   * RPC to load it and cause to load the non-exist matcher class again. JVM will report
   * `ClassCircularityError` to prevent such infinite recursion. (See SPARK-17714)
   */
  //网络层数据的加密，MessageEncoder实例
  private static final MessageEncoder ENCODER = MessageEncoder.INSTANCE;
  //网络层数据的解密，MessageDecoder实例
  private static final MessageDecoder DECODER = MessageDecoder.INSTANCE;

  public TransportContext(TransportConf conf, RpcHandler rpcHandler) {
    this(conf, rpcHandler, false);
  }

  public TransportContext(
      TransportConf conf,
      RpcHandler rpcHandler,
      boolean closeIdleConnections) {
    this.conf = conf;
    this.rpcHandler = rpcHandler;
    this.closeIdleConnections = closeIdleConnections;
  }

  /**
   * Initializes a ClientFactory which runs the given TransportClientBootstraps prior to returning
   * a new Client. Bootstraps will be executed synchronously, and must run successfully in order
   * to create a Client.
   */
  public TransportClientFactory createClientFactory(List<TransportClientBootstrap> bootstraps) {
    return new TransportClientFactory(this, bootstraps);
  }

  public TransportClientFactory createClientFactory() {
    return createClientFactory(new ArrayList<>());
  }

  /** Create a server which will attempt to bind to a specific port. */
  public TransportServer createServer(int port, List<TransportServerBootstrap> bootstraps) {
    return new TransportServer(this, null, port, rpcHandler, bootstraps);
  }

  /** Create a server which will attempt to bind to a specific host and port. */
  public TransportServer createServer(
      String host, int port, List<TransportServerBootstrap> bootstraps) {
    return new TransportServer(this, host, port, rpcHandler, bootstraps);
  }

  /** Creates a new server, binding to any available ephemeral port. */
  public TransportServer createServer(List<TransportServerBootstrap> bootstraps) {
    return createServer(0, bootstraps);
  }

  public TransportServer createServer() {
    return createServer(0, new ArrayList<>());
  }

  public TransportChannelHandler initializePipeline(SocketChannel channel) {
    return initializePipeline(channel, rpcHandler);
  }

  /**
   * Initializes a client or server Netty Channel Pipeline which encodes/decodes messages and
   * has a {@link org.apache.spark.network.server.TransportChannelHandler} to handle request or
   * response messages.
   *
   * @param channel The channel to initialize.
   * @param channelRpcHandler The RPC handler to use for the channel.
   *
   * @return Returns the created TransportChannelHandler, which includes a TransportClient that can
   * be used to communicate on this channel. The TransportClient is directly associated with a
   * ChannelHandler to ensure all users of the same channel get the same TransportClient object.
   */
  public TransportChannelHandler initializePipeline(
      SocketChannel channel,
      RpcHandler channelRpcHandler) {
    try {
      TransportChannelHandler channelHandler = createChannelHandler(channel, channelRpcHandler);
      channel.pipeline()
        .addLast("encoder", ENCODER)
        .addLast(TransportFrameDecoder.HANDLER_NAME, NettyUtils.createFrameDecoder())
        .addLast("decoder", DECODER)
        // 处理心跳的handler
        .addLast("idleStateHandler", new IdleStateHandler(0, 0, conf.connectionTimeoutMs() / 1000))
        // NOTE: Chunks are currently guaranteed to be returned in the order of request, but this
        // would require more logic to guarantee if this were not part of the same event loop.
        .addLast("handler", channelHandler);
      return channelHandler;
    } catch (RuntimeException e) {
      logger.error("Error while initializing Netty pipeline", e);
      throw e;
    }
  }

  /**
   * Creates the server- and client-side handler which is used to handle both RequestMessages and
   * ResponseMessages. The channel is expected to have been successfully created, though certain
   * properties (such as the remoteAddress()) may not be available yet.
   */
  private TransportChannelHandler createChannelHandler(Channel channel, RpcHandler rpcHandler) {
    TransportResponseHandler responseHandler = new TransportResponseHandler(channel);
    TransportClient client = new TransportClient(channel, responseHandler);
    TransportRequestHandler requestHandler = new TransportRequestHandler(channel, client,
      rpcHandler, conf.maxChunksBeingTransferred());
    return new TransportChannelHandler(client, responseHandler, requestHandler,
      conf.connectionTimeoutMs(), closeIdleConnections);
  }

  public TransportConf getConf() { return conf; }
}
