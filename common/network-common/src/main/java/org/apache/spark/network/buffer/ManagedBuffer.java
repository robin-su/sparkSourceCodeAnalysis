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

package org.apache.spark.network.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * 该接口为字节形式的数据提供了一个不可变的视图。实现应该指定如何提供数据：
 * - FileSegmentManagedBuffer：由文件的一部分支持的数据
 * - NioManagedBuffer：由 NIO ByteBuffer 支持的数据
 * - NettyManagedBuffer：由 Netty ByteBuf 支持的数据，具体的缓冲区实现可能在 JVM 垃圾收集器之外进行管理.
 *   例如，在 NettyManagedBuffer 的情况下，缓冲区是引用计数的。在这种情况下，如果缓冲区要传递给不同的线程，则应调用保留/释放。
 *
 *
 * This interface provides an immutable view for data in the form of bytes. The implementation
 * should specify how the data is provided:
 *
 * - {@link FileSegmentManagedBuffer}: data backed by part of a file
 * - {@link NioManagedBuffer}: data backed by a NIO ByteBuffer
 * - {@link NettyManagedBuffer}: data backed by a Netty ByteBuf
 *
 * The concrete buffer implementation might be managed outside the JVM garbage collector.
 * For example, in the case of {@link NettyManagedBuffer}, the buffers are reference counted.
 * In that case, if the buffer is going to be passed around to a different thread, retain/release
 * should be called.
 */
public abstract class ManagedBuffer {

  /**
   * 数据的字节数。如果此缓冲区将所有视图解密到数据中，则这是解密数据的大小。
   *
   * Number of bytes of the data. If this buffer will decrypt for all of the views into the data,
   * this is the size of the decrypted data.
   */
  public abstract long size();

  /**
   * 将数据按照NIO的ByteBuffer类型返回
   *
   * 将缓冲区的数据暴露给Nio ByteBuffer(字节缓冲区)。改变返回的ByteBuffer的position和limit属性，但其不会对
   * 缓冲区的内容造成影响
   *
   * Exposes this buffer's data as an NIO ByteBuffer. Changing the position and limit of the
   * returned ByteBuffer should not affect the content of this buffer.
   */
  // TODO: Deprecate this, usage may require expensive memory mapping or allocation.
  public abstract ByteBuffer nioByteBuffer() throws IOException;

  /**
   * 将缓冲区的数据以InputStream(输入流)的形式暴露出去，底层实现不需要检查读取的字节长度，所以调用者有责任确保它不会超过限制。
   *
   * Exposes this buffer's data as an InputStream. The underlying implementation does not
   * necessarily check for the length of bytes read, so the caller is responsible for making sure
   * it does not go over the limit.
   */
  public abstract InputStream createInputStream() throws IOException;

  /**
   * 如果适用，将引用计数加一。
   * 当有新的使用者使用此视图时，增加引用视图的引用数
   *
   * Increment the reference count by one if applicable.
   */
  public abstract ManagedBuffer retain();

  /**
   * 当有使用者不在使用此视图时，减少引用此视图的引用数，当引用数为0时释放缓冲区
   *
   *
   * If applicable, decrement the reference count by one and deallocates the buffer if the
   * reference count reaches zero.
   */
  public abstract ManagedBuffer release();

  /**
   * 将缓冲区转换为 Netty 对象，用于将数据写出。返回值是 io.netty.buffer.ByteBuf 或 io.netty.channel.FileRegion。
   * 如果此方法返回 ByteBuf，则该缓冲区的引用计数将增加，调用者将负责释放此新引用。
   *
   * Convert the buffer into an Netty object, used to write the data out. The return value is either
   * a {@link io.netty.buffer.ByteBuf} or a {@link io.netty.channel.FileRegion}.
   *
   * If this method returns a ByteBuf, then that buffer's reference count will be incremented and
   * the caller will be responsible for releasing this new reference.
   */
  public abstract Object convertToNetty() throws IOException;
}
