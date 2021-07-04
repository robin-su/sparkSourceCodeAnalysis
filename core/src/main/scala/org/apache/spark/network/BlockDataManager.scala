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

package org.apache.spark.network

import scala.reflect.ClassTag

import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.client.StreamCallbackWithID
import org.apache.spark.storage.{BlockId, StorageLevel}

private[spark]
trait BlockDataManager {

  /**
   * 获取本地Block块的接口，若block不可读或者不可写，则抛出异常
   *
   * Interface to get local block data. Throws an exception if the block cannot be found or
   * cannot be read successfully.
   */
  def getBlockData(blockId: BlockId): ManagedBuffer

  /**
   * 根据存储级别，将block放入到本地
   *
   * Put the block locally, using the given storage level.
   *
   * 如果块已存储，则返回 true，如果放置操作失败或块已存在，则返回 false。
   * Returns true if the block was stored and false if the put operation failed or the block
   * already existed.
   */
  def putBlockData(
      blockId: BlockId,
      data: ManagedBuffer,
      level: StorageLevel,
      classTag: ClassTag[_]): Boolean

  /**
   * 放置将作为流接收的给定块。
     调用此方法时，块数据本身不可用——它将传递给返回的 StreamCallbackWithID。

   * Put the given block that will be received as a stream.
   *
   * When this method is called, the block data itself is not available -- it will be passed to the
   * returned StreamCallbackWithID.
   */
  def putBlockDataAsStream(
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[_]): StreamCallbackWithID

  /**
   * 释放由 [[putBlockData()]] 和 [[getBlockData()]] 获取的锁。
   *
   * Release locks acquired by [[putBlockData()]] and [[getBlockData()]].
   */
  def releaseLock(blockId: BlockId, taskAttemptId: Option[Long]): Unit
}
