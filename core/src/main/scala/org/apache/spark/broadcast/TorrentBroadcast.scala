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

package org.apache.spark.broadcast

import java.io._
import java.nio.ByteBuffer
import java.util.zip.Adler32

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.Random

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage._
import org.apache.spark.util.Utils
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

/**
 * 广播变量一种类BitTorrent的实现方式
 * A BitTorrent-like implementation of [[org.apache.spark.broadcast.Broadcast]].
 *
 * 以下是广播变量的描述：
 * The mechanism is as follows:
 *
 * driver会将序列化对象拆分成多个小小的块(chunks)，并将这些小块保存到Driver端的BlockManager中
 * The driver divides the serialized object into small chunks and
 * stores those chunks in the BlockManager of the driver.
 *
 * 对于每个Executor，它首先尝试从自己的BlockManager中获取数据，若在它不存在，则它会向处在远端可达的Driver或者其他Executor
 * 抓取数据，一旦它抓取到数据块，它就会将其放到自己的BlockManager中，其他executor也可以从它这里抓取数据
 * On each executor, the executor first attempts to fetch the object from its BlockManager. If
 * it does not exist, it then uses remote fetches to fetch the small chunks from the driver and/or
 * other executors if available. Once it gets the chunks, it puts the chunks in its own
 * BlockManager, ready for other executors to fetch from.
 *
 * 这种机制可以防止Driver端向每个Executor段发送副本锁带来的性能瓶劲。（即每个executor主动拉去，而不是Driver发送）
 * 在超大规模集群模式下，能有效的防止Driver发送过多的Executor所带来的瓶劲
 * This prevents the driver from being the bottleneck in sending out multiple copies of the
 * broadcast data (one per executor).
 *
 * When initialized, TorrentBroadcast objects read SparkEnv.get.conf.
 *
 * @param obj object to broadcast
 * @param id A unique identifier for the broadcast variable.
 */
private[spark] class TorrentBroadcast[T: ClassTag](obj: T, id: Long)
  extends Broadcast[T](id) with Logging with Serializable {

  /**
   * 从Executor或者Driver上读取的广播块的值。_value是通过调用read-BroadcastBlock方法获得的广播对象。
   * 由于_value是个lazy及val修饰的属性，因此在构造TorrentBroadcast实例的时候不会调用readBroadcastBlock方法，
   * 而是等到明确需要使用_value的值时。
   *
   * executor中广播变量的值，通过readBroadcastBlock方法进行重建，通过从Driver或者Executors中读取blocks来构建value.
   * 对于Driver，若需要广播变量，则以延迟读取的方式从块管理器（BlockManager）中读取。
   *
   * Value of the broadcast object on executors. This is reconstructed by [[readBroadcastBlock]],
   * which builds this value by reading blocks from the driver and/or other executors.
   *
   * On the driver, if the value is required, it is read lazily from the block manager.
   */
  @transient private lazy val _value: T = readBroadcastBlock()

  /** 广播变量的压缩方式，若为None，则说明不需要压缩广播变量 **/
  /** The compression codec to use, or None if compression is disabled */
  @transient private var compressionCodec: Option[CompressionCodec] = _

  /** block块的大小，默认是4MB，该变量仅仅被广播者锁读取 **/
  /** Size of each block. Default value is 4MB.  This value is only read by the broadcaster. */
  @transient private var blockSize: Int = _

  private def setConf(conf: SparkConf) {
    compressionCodec = if (conf.getBoolean("spark.broadcast.compress", true)) {
      Some(CompressionCodec.createCodec(conf))
    } else {
      None
    }
    // Note: use getSizeAsKb (not bytes) to maintain compatibility if no units are provided
    blockSize = conf.getSizeAsKb("spark.broadcast.blockSize", "4m").toInt * 1024
    checksumEnabled = conf.getBoolean("spark.broadcast.checksum", true)
  }
  setConf(SparkEnv.get.conf)

  // 获取广播变量ID
  private val broadcastId = BroadcastBlockId(id)

  /**
   * blocks块的数量
   */
  /** Total number of blocks this broadcast variable contains. */
  private val numBlocks: Int = writeBlocks(obj)

  /**
   * 是否给广播块生成校验和
   */
  /** Whether to generate checksum for blocks or not. */
  private var checksumEnabled: Boolean = false
  /**
   * 用于存储每个广播块的校验和的数组。
   */
  /** The checksum for all the blocks. */
  private var checksums: Array[Int] = _

  /**
   *
   * @return
   */
  override protected def getValue() = {
    _value
  }

  private def calcChecksum(block: ByteBuffer): Int = {
    val adler = new Adler32()
    if (block.hasArray) {
      adler.update(block.array, block.arrayOffset + block.position(), block.limit()
        - block.position())
    } else {
      val bytes = new Array[Byte](block.remaining())
      block.duplicate.get(bytes)
      adler.update(bytes)
    }
    adler.getValue.toInt
  }

  /**
   * 将对象分割成许多块(blocks),并将其存储到block manager中
   * Divide the object into multiple blocks and put those blocks in the block manager.
   *
   * @param value the object to divide
   * @return number of blocks this broadcast variable is divided into
   */
  private def writeBlocks(value: T): Int = {
    import StorageLevel._
    // Store a copy of the broadcast variable in the driver so that tasks run on the driver
    // do not create a duplicate copy of the broadcast variable's value.
    // 存储一份广播变量的拷贝，方便driver的任务快速读取广播变量
    val blockManager = SparkEnv.get.blockManager
    // 调用BlockManager的putSingle方法将广播对象写入本地的存储体系
    if (!blockManager.putSingle(broadcastId, value, MEMORY_AND_DISK, tellMaster = false)) {
      throw new SparkException(s"Failed to store $broadcastId in BlockManager")
    }
    // 序列化数据对象，并拆分成多个数据块
    val blocks =
      TorrentBroadcast.blockifyObject(value, blockSize, SparkEnv.get.serializer, compressionCodec)
    if (checksumEnabled) {
      checksums = new Array[Int](blocks.length)
    }

    // 存储数据块到BlockManager
    blocks.zipWithIndex.foreach { case (block, i) =>
      if (checksumEnabled) {
        // 如果开启校验功能，计算数据块的校验和，使用Adler-32算法，
        // Adler-32校验和几乎与CRC-3一样可靠，但是能够更快地计算出来。
        checksums(i) = calcChecksum(block)
      }
      val pieceId = BroadcastBlockId(id, "piece" + i)
      val bytes = new ChunkedByteBuffer(block.duplicate())
      // 写入广播数据块到driver端的BlockManager
      if (!blockManager.putBytes(pieceId, bytes, MEMORY_AND_DISK_SER, tellMaster = true)) {
        throw new SparkException(s"Failed to store $pieceId of $broadcastId in local BlockManager")
      }
    }
    blocks.length
  }

  /** Fetch torrent blocks from the driver and/or other executors. */
  private def readBlocks(): Array[BlockData] = {
    // Fetch chunks of data. Note that all these chunks are stored in the BlockManager and reported
    // to the driver, so other executors can pull these chunks from this executor as well.
    val blocks = new Array[BlockData](numBlocks)
    // 从SparkEnv中获取BlockManager
    val bm = SparkEnv.get.blockManager

    for (pid <- Random.shuffle(Seq.range(0, numBlocks))) {
      val pieceId = BroadcastBlockId(id, "piece" + pid)
      logDebug(s"Reading piece $pieceId of $broadcastId")

      /**
       * 首先尝试从本地获取block块，因为之前的尝试获取可能已经获取到了一部分block，因此这部分block块在本地可能获取到。
       */
      //       First try getLocalBytes because there is a chance that previous attempts to fetch the
//       broadcast blocks have already fetched some of the blocks. In that case, some blocks
//       would be available locally (on this executor).
      bm.getLocalBytes(pieceId) match {
        case Some(block) =>
          blocks(pid) = block
          releaseLock(pieceId)
        case None =>
//          获取远端driver或executor的数据块，BlockManager会向driver端的BlockManager查询存储该数据块的所有executor和driver位置，
        // 然后随机获取其中一个位置拉取数据
          bm.getRemoteBytes(pieceId) match {
            case Some(b) =>
              if (checksumEnabled) {
                val sum = calcChecksum(b.chunks(0))
                if (sum != checksums(pid)) {
                  throw new SparkException(s"corrupt remote block $pieceId of $broadcastId:" +
                    s" $sum != ${checksums(pid)}")
                }
              }
              // We found the block from remote executors/driver's BlockManager, so put the block
              // in this executor's BlockManager.
              if (!bm.putBytes(pieceId, b, StorageLevel.MEMORY_AND_DISK_SER, tellMaster = true)) {
                throw new SparkException(
                  s"Failed to store $pieceId of $broadcastId in local BlockManager")
              }
              blocks(pid) = new ByteBufferBlockData(b, true)
            case None =>
              throw new SparkException(s"Failed to get $pieceId of $broadcastId")
          }
      }
    }
    blocks
  }

  /**
   * Remove all persisted state associated with this Torrent broadcast on the executors.
   */
  override protected def doUnpersist(blocking: Boolean) {
    TorrentBroadcast.unpersist(id, removeFromDriver = false, blocking)
  }

  /**
   * Remove all persisted state associated with this Torrent broadcast on the executors
   * and driver.
   */
  override protected def doDestroy(blocking: Boolean) {
    TorrentBroadcast.unpersist(id, removeFromDriver = true, blocking)
  }

  /** Used by the JVM when serializing this object. */
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    assertValid()
    out.defaultWriteObject()
  }

  private def readBroadcastBlock(): T = Utils.tryOrIOException {
    TorrentBroadcast.synchronized {
      // 从broadcastManager中换取广播变量缓存
      val broadcastCache = SparkEnv.get.broadcastManager.cachedValues
//      从blockcastManager中获取广播变量信息，若不存在
      Option(broadcastCache.get(broadcastId)).map(_.asInstanceOf[T]).getOrElse {
        setConf(SparkEnv.get.conf)
//        再获取blockManager
        val blockManager = SparkEnv.get.blockManager
//        从blockManager中获取本地广播变量
        blockManager.getLocalValues(broadcastId) match {
          case Some(blockResult) =>
            if (blockResult.data.hasNext) {
              val x = blockResult.data.next().asInstanceOf[T]
              releaseLock(broadcastId)

              if (x != null) {
                // 写入到bolckcastManager
                broadcastCache.put(broadcastId, x)
              }

              x
            } else {
              throw new SparkException(s"Failed to get locally stored broadcast data: $broadcastId")
            }
          case None =>
            logInfo("Started reading broadcast variable " + id)
            val startTimeMs = System.currentTimeMillis()
            val blocks = readBlocks()
            logInfo("Reading broadcast variable " + id + " took" + Utils.getUsedTimeMs(startTimeMs))

            try {
              val obj = TorrentBroadcast.unBlockifyObject[T](
                blocks.map(_.toInputStream()), SparkEnv.get.serializer, compressionCodec)
              // Store the merged copy in BlockManager so other tasks on this executor don't
              // need to re-fetch it.
              val storageLevel = StorageLevel.MEMORY_AND_DISK
              if (!blockManager.putSingle(broadcastId, obj, storageLevel, tellMaster = false)) {
                throw new SparkException(s"Failed to store $broadcastId in BlockManager")
              }

              if (obj != null) {
                broadcastCache.put(broadcastId, obj)
              }

              obj
            } finally {
              blocks.foreach(_.dispose())
            }
        }
      }
    }
  }

  /**
   * If running in a task, register the given block's locks for release upon task completion.
   * Otherwise, if not running in a task then immediately release the lock.
   */
  private def releaseLock(blockId: BlockId): Unit = {
    val blockManager = SparkEnv.get.blockManager
    Option(TaskContext.get()) match {
      case Some(taskContext) =>
        taskContext.addTaskCompletionListener[Unit](_ => blockManager.releaseLock(blockId))
      case None =>
        // This should only happen on the driver, where broadcast variables may be accessed
        // outside of running tasks (e.g. when computing rdd.partitions()). In order to allow
        // broadcast variables to be garbage collected we need to free the reference here
        // which is slightly unsafe but is technically okay because broadcast variables aren't
        // stored off-heap.
        blockManager.releaseLock(blockId)
    }
  }

}


private object TorrentBroadcast extends Logging {

  def blockifyObject[T: ClassTag](
      obj: T,
      blockSize: Int,
      serializer: Serializer,
      compressionCodec: Option[CompressionCodec]): Array[ByteBuffer] = {
    val cbbos = new ChunkedByteBufferOutputStream(blockSize, ByteBuffer.allocate)
    val out = compressionCodec.map(c => c.compressedOutputStream(cbbos)).getOrElse(cbbos)
    val ser = serializer.newInstance()
    val serOut = ser.serializeStream(out)
    Utils.tryWithSafeFinally {
      serOut.writeObject[T](obj)
    } {
      serOut.close()
    }
    cbbos.toChunkedByteBuffer.getChunks()
  }

  def unBlockifyObject[T: ClassTag](
      blocks: Array[InputStream],
      serializer: Serializer,
      compressionCodec: Option[CompressionCodec]): T = {
    require(blocks.nonEmpty, "Cannot unblockify an empty array of blocks")
    val is = new SequenceInputStream(blocks.iterator.asJavaEnumeration)
    val in: InputStream = compressionCodec.map(c => c.compressedInputStream(is)).getOrElse(is)
    val ser = serializer.newInstance()
    val serIn = ser.deserializeStream(in)
    val obj = Utils.tryWithSafeFinally {
      serIn.readObject[T]()
    } {
      serIn.close()
    }
    obj
  }

  /**
   * Remove all persisted blocks associated with this torrent broadcast on the executors.
   * If removeFromDriver is true, also remove these persisted blocks on the driver.
   */
  def unpersist(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit = {
    logDebug(s"Unpersisting TorrentBroadcast $id")
    SparkEnv.get.blockManager.master.removeBroadcast(id, removeFromDriver, blocking)
  }
}
