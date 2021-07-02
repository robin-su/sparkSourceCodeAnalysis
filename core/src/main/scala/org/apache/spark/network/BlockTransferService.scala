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

import java.io.Closeable
import java.nio.ByteBuffer

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.{BlockFetchingListener, DownloadFileManager, ShuffleClient}
import org.apache.spark.storage.{BlockId, EncryptedManagedBuffer, StorageLevel}
import org.apache.spark.util.ThreadUtils

/**
 * 它是ShuffleClient的子类。它是ShuffleClient的抽象实现类，定义了读取shuffle的基础框架。
 */
private[spark]
abstract class BlockTransferService extends ShuffleClient with Closeable with Logging {

  /**
   * 它额外提供了使用BlockDataManager初始化的方法，方便从本地获取block或者将block存入本地。
   *
   * Initialize the transfer service by giving it the BlockDataManager that can be used to fetch
   * local blocks or put local blocks.
   */
  def init(blockDataManager: BlockDataManager): Unit

  /**
   * 关闭ShuffleClient
   *
   * Tear down the transfer service.
   */
  def close(): Unit

  /**
   * 服务正在监听的端口
   *
   * Port number the service is listening on, available only after [[init]] is invoked.
   */
  def port: Int

  /**
   * 服务正在监听的hostname
   *
   * Host name the service is listening on, available only after [[init]] is invoked.
   */
  def hostName: String

  /**
   * 跟继承类一样，没有实现，由于继承关系可以不写。
   *
   * Fetch a sequence of blocks from a remote node asynchronously,
   * available only after [[init]] is invoked.
   *
   * Note that this API takes a sequence so the implementation can batch requests, and does not
   * return a future so the underlying implementation can invoke onBlockFetchSuccess as soon as
   * the data of a block is fetched, rather than waiting for all blocks to be fetched.
   */
  override def fetchBlocks(
      host: String,
      port: Int,
      execId: String,
      blockIds: Array[String],
      listener: BlockFetchingListener,
      tempFileManager: DownloadFileManager): Unit

  /**
   * 上传block到远程节点，返回一个future对象
   *
   * Upload a single block to a remote node, available only after [[init]] is invoked.
   */
  def uploadBlock(
      hostname: String,
      port: Int,
      execId: String,
      blockId: BlockId,
      blockData: ManagedBuffer,
      level: StorageLevel,
      classTag: ClassTag[_]): Future[Unit]

  /**
   * 同步抓取远程节点的block，直到block数据获取成功才返回
   *
   * A special case of [[fetchBlocks]], as it fetches only one block and is blocking.
   *
   * It is also only available after [[init]] is invoked.
   */
  def fetchBlockSync(
      host: String,
      port: Int,
      execId: String,
      blockId: String,
      tempFileManager: DownloadFileManager): ManagedBuffer = {
    // A monitor for the thread to wait on.
    val result = Promise[ManagedBuffer]()
    fetchBlocks(host, port, execId, Array(blockId),
      new BlockFetchingListener {
        // 若拉取block失败
        override def onBlockFetchFailure(blockId: String, exception: Throwable): Unit = {
          result.failure(exception)
        }
        // 拉取block成功调用
        override def onBlockFetchSuccess(blockId: String, data: ManagedBuffer): Unit = {
          data match {
            case f: FileSegmentManagedBuffer =>
              result.success(f)
            case e: EncryptedManagedBuffer =>
              result.success(e)
            case _ =>
              try {
                // 使用Nio字节缓存ByteBuffer，将数据写入缓存
                val ret = ByteBuffer.allocate(data.size.toInt)
                ret.put(data.nioByteBuffer())
                ret.flip()
                result.success(new NioManagedBuffer(ret))
              } catch {
                case e: Throwable => result.failure(e)
              }
          }
        }
      }, tempFileManager)
    ThreadUtils.awaitResult(result.future, Duration.Inf)
  }

  /**
   * uploadBlockSync 方法：同步上传信息，直到上传成功才结束
   *
   * Upload a single block to a remote node, available only after [[init]] is invoked.
   *
   * This method is similar to [[uploadBlock]], except this one blocks the thread
   * until the upload finishes.
   */
  def uploadBlockSync(
      hostname: String,
      port: Int,
      execId: String,
      blockId: BlockId,
      blockData: ManagedBuffer,
      level: StorageLevel,
      classTag: ClassTag[_]): Unit = {
    val future = uploadBlock(hostname, port, execId, blockId, blockData, level, classTag)
    ThreadUtils.awaitResult(future, Duration.Inf)
  }
}
