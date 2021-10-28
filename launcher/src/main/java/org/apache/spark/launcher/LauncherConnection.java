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

package org.apache.spark.launcher;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.spark.launcher.LauncherProtocol.*;

/**
 * Encapsulates a connection between a launcher server and client. This takes care of the
 * communication (sending and receiving messages), while processing of messages is left for
 * the implementations.
 */
abstract class LauncherConnection implements Closeable, Runnable {

  private static final Logger LOG = Logger.getLogger(LauncherConnection.class.getName());

  private final Socket socket;
  private final ObjectOutputStream out;

  private volatile boolean closed;

  LauncherConnection(Socket socket) throws IOException {
    this.socket = socket;
    this.out = new ObjectOutputStream(socket.getOutputStream());
    this.closed = false;
  }

  protected abstract void handle(Message msg) throws IOException;

  /**
   * 由于LauncherConnection实现了java.lang.Runnable接口，因此需要实现run方法。LauncherConnection的run方法
   * 用于从Socket客户端的输入流中读取LauncherServer发送的消息，并调用handle方法对消息进行处理。LauncherConnection的run方法同时也是一个模板方法。
   */
  @Override
  public void run() {
    try {
      FilteredObjectInputStream in = new FilteredObjectInputStream(socket.getInputStream());
      while (isOpen()) {
        Message msg = (Message) in.readObject();
        handle(msg);
      }
    } catch (EOFException eof) {
      // Remote side has closed the connection, just cleanup.
      try {
        close();
      } catch (Exception unused) {
        // no-op.
      }
    } catch (Exception e) {
      if (!closed) {
        LOG.log(Level.WARNING, "Error in inbound message handling.", e);
        try {
          close();
        } catch (Exception unused) {
          // no-op.
        }
      }
    }
  }

  /**
   * 此方法通过Socket客户端与LauncherServer的Socket服务端建立的连接向LauncherServer发送消息
   *
   * @param msg
   * @throws IOException
   */
  protected synchronized void send(Message msg) throws IOException {
    try {
      CommandBuilderUtils.checkState(!closed, "Disconnected.");
      out.writeObject(msg);
      out.flush();
    } catch (IOException ioe) {
      if (!closed) {
        LOG.log(Level.WARNING, "Error when sending message.", ioe);
        try {
          close();
        } catch (Exception unused) {
          // no-op.
        }
      }
      throw ioe;
    }
  }

  /**
   * 此方法用于关闭Socket客户端与LauncherServer的Socket服务端建立的连接
   * @throws IOException
   */
  @Override
  public synchronized void close() throws IOException {
    if (isOpen()) {
      closed = true;
      socket.close();
    }
  }

  boolean isOpen() {
    return !closed;
  }

}
