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

package org.apache.spark.shuffle

import java.io._
import java.nio.channels.Channels
import java.nio.file.Files

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.io.NioBufferedFileInputStream
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.storage._
import org.apache.spark.util.Utils

/**
 * Create and maintain the shuffle blocks' mapping between logic block and physical file location.
 * Data of shuffle blocks from the same map task are stored in a single consolidated data file.
 * The offsets of the data blocks in the data file are stored in a separate index file.
 *
 * We use the name of the shuffle data's shuffleBlockId with reduce ID set to 0 and add ".data"
 * as the filename postfix for data file, and ".index" as the filename postfix for index file.
 *
 */
// Note: Changes to the format in this file should be kept in sync with
// org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getSortBasedShuffleBlockData().
private[spark] class IndexShuffleBlockResolver(
    conf: SparkConf,
    _blockManager: BlockManager = null)
  extends ShuffleBlockResolver
  with Logging {

  private lazy val blockManager = Option(_blockManager).getOrElse(SparkEnv.get.blockManager)
  /**
   * 方便Shuffle客户端传输线程和服务端传输线程进行读取
   */
  private val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle")

  /**
   * 获取Shuffle数据文件
   *
   * @param shuffleId
   * @param mapId
   * @return
   */
  def getDataFile(shuffleId: Int, mapId: Int): File = {
    blockManager.diskBlockManager.getFile(ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID))
  }

  /**
   * 获取Shuffle索引文件
   *
   * @param shuffleId
   * @param mapId
   * @return
   */
  private def getIndexFile(shuffleId: Int, mapId: Int): File = {
    blockManager.diskBlockManager.getFile(ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID))
  }

  /**
   * Remove data file and index file that contain the output data from one map.
   */
  def removeDataByMap(shuffleId: Int, mapId: Int): Unit = {
    var file = getDataFile(shuffleId, mapId)
    if (file.exists()) {
      if (!file.delete()) {
        logWarning(s"Error deleting data ${file.getPath()}")
      }
    }

    file = getIndexFile(shuffleId, mapId)
    if (file.exists()) {
      if (!file.delete()) {
        logWarning(s"Error deleting index ${file.getPath()}")
      }
    }
  }

  /**
   * 对给定的索引文件和数据文件是否匹配进行检查。
   * checkIndexAndDataFile方法最后将返回索引文件中各个partition的长度数据existingLengths。
   *
   * Check whether the given index and data files match each other.
   * If so, return the partition lengths in the data file. Otherwise return null.
   */
  private def checkIndexAndDataFile(index: File, data: File, blocks: Int): Array[Long] = {
    // the index file should have `block + 1` longs as offset.
    // 索引文件的长度不等于 (块+1 * 8L),索引值为Long型，占8个字节，索引文件头的8个字节为标记值，即Long型整数0，不算索引值
    if (index.length() != (blocks + 1) * 8L) {
      return null
    }
    val lengths = new Array[Long](blocks)
    // Read the lengths of blocks
    val in = try {
      // 使用NIO读取索引文件
      new DataInputStream(new NioBufferedFileInputStream(index))
    } catch {
      case e: IOException =>
        return null
    }
    try {
      // Convert the offsets into lengths of each block
      // 读取第一个Long型整数,第一个就是标记字节
      var offset = in.readLong()
      //  第一个Long型整数标记值必须为0，如果不为0，则直接返回null
      if (offset != 0L) {
        return null
      }
      var i = 0
      while (i < blocks) {
        // 注意再读的时候，偏移量已经发生了变化
        val off = in.readLong()
        // 记录对应的数据块长度
        lengths(i) = off - offset
        // 更新offset
        offset = off
        // 继续下一次
        i += 1
      }
    } catch {
      case e: IOException =>
        return null
    } finally {
      in.close()
    }

    // 数据文件的长度，等于lengths数组所有元素之和，表示校验成功
    // the size of data file should match with index file
    if (data.length() == lengths.sum) {
      // 返回lengths数组
      lengths
    } else {
      null
    }
  }

  /**
   * 用于将每个Block的偏移量写入索引文件
   *
   * 索引文件存储的是每个Reduce任务起始位置在数据文件中的偏移位置，索引值为Long型，占8个字节，索引文件头的8个字节为标记值，即Long型整数0，
   * 所以索引文件的其长度需要是【数据块数量 + 1】 * 8，紧邻的两个索引值的差为对应Reduce的数据的大小，检查时候我们需要保证两方面一是索引文件数据是
   * 【数据块数量 + 1】 * 8，另一方面要保证读取索引得到的数据文件的大小等于数据文件读取到的大小。
   *
   *
   * 先根据偏移量情况写偏移量到索引临时文件中；
     获取索引文件和数据文件，然后加锁，检查indexFile文件、dataFile文件及数据块的长度是否相同，这个操作是为了检查可能已经存在的索引文件与数据文件是否匹配，
     如果检查发现时匹配的，则说明有其他的TaskAttempt已经完成了该Map任务的写出，那么此时产生的临时索引文件就无用了。
     如果匹配成功说明其他任务已经写入了，删除临时的索引文件和数据文件；
     如果不匹配，将临时文件重命名为正式文件
   *
   * Write an index file with the offsets of each block, plus a final offset at the end for the
   * end of the output file. This will be used by getBlockData to figure out where each block
   * begins and ends.
   *
   * It will commit the data and index file as an atomic operation, use the existing ones, or
   * replace them with new ones.
   *
   * Note: the `lengths` will be updated to match the existing index file if use the existing ones.
   */
  def writeIndexFileAndCommit(
      shuffleId: Int,
      mapId: Int,
      lengths: Array[Long],
      dataTmp: File): Unit = {
    // 获取指定Shuffle中指定map任务输出的索引文件
    val indexFile = getIndexFile(shuffleId, mapId)
    // 根据索引文件获取临时文件的路径
    val indexTmp = Utils.tempFileWith(indexFile)
    try {
      // 获取指定shuffle中指定map任务输出的数据文件
      val dataFile = getDataFile(shuffleId, mapId)
      // There is only one IndexShuffleBlockResolver per executor, this synchronization make sure
      // the following check and rename are atomic.
      synchronized {
        // 校验索引文件和数据文件是否匹配
        val existingLengths = checkIndexAndDataFile(indexFile, dataFile, lengths.length)
        if (existingLengths != null) {
          // Another attempt for the same task has already written our map outputs successfully,
          // so just use the existing partition lengths and delete our temporary map outputs.
          /**
           * existingLengths： 原数组
           * 0： 原数组要复制的起始位置
           * lengths： 目标属组
           * 0： 目标数组要放置的起始位置
           * lengths.length： 复制的长度
           */
          System.arraycopy(existingLengths, 0, lengths, 0, lengths.length)
          if (dataTmp != null && dataTmp.exists()) {
            // 将临时文件删除
            dataTmp.delete()
          }
        } else {
          // This is the first successful attempt in writing the map outputs for this task,
          // so override any existing index and data files with the ones we wrote.
          // 构建临时文件的输出流
          val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexTmp)))
          Utils.tryWithSafeFinally {
            // We take in lengths of each block, need to convert it to offsets.
            // 写入临时索引文件的第一个标记值，即Long型整数0
            var offset = 0L
            out.writeLong(offset)
            // 遍历每个数据块的长度，并作为偏移量写入临时索引文件
            for (length <- lengths) {
              // 在原有offset上加上数据块的长度
              offset += length
              // 写入临时文件
              out.writeLong(offset)
            }
          } {
            out.close()
          }

          if (indexFile.exists()) {
            indexFile.delete()
          }
          if (dataFile.exists()) {
            dataFile.delete()
          }
          // 临时的索引文件和数据文件作为正式的索引文件和数据文件, 将indexTmp重命名为indexFile
          if (!indexTmp.renameTo(indexFile)) {
            throw new IOException("fail to rename file " + indexTmp + " to " + indexFile)
          }
          // 将dataTmp重命名为dataFile
          if (dataTmp != null && dataTmp.exists() && !dataTmp.renameTo(dataFile)) {
            throw new IOException("fail to rename file " + dataTmp + " to " + dataFile)
          }
        }
      }
    } finally {
      // 如果临时索引文件还存在，一定要将其删除
      if (indexTmp.exists() && !indexTmp.delete()) {
        logError(s"Failed to delete temporary index file at ${indexTmp.getAbsolutePath}")
      }
    }
  }

  /**
   * 获取Block数据
   *
   * @param blockId
   * @return
   */
  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
    // The block is actually going to be a range of a single map output file for this map, so
    // find out the consolidated file, then the offset within that from our index
    // 获取指定map任务输出的索引文件
    val indexFile = getIndexFile(blockId.shuffleId, blockId.mapId)

    // SPARK-22982: if this FileInputStream's position is seeked forward by another piece of code
    // which is incorrectly using our file descriptor then this code will fetch the wrong offsets
    // (which may cause a reducer to be sent a different reducer's data). The explicit position
    // checks added here were a useful debugging aid during SPARK-22982 and may help prevent this
    // class of issue from re-occurring in the future which is why they are left here even though
    // SPARK-22982 is fixed.
    val channel = Files.newByteChannel(indexFile.toPath)
    // 跳过与当前reduce任务无关的字节
    channel.position(blockId.reduceId * 8L)
    // 读取索引文件的输入流
    val in = new DataInputStream(Channels.newInputStream(channel))
    try {
      // 读取偏移量
      val offset = in.readLong()
      // 读取下一个偏移量
      val nextOffset = in.readLong()
      val actualPosition = channel.position()
      val expectedPosition = blockId.reduceId * 8L + 16
      if (actualPosition != expectedPosition) {
        throw new Exception(s"SPARK-22982: Incorrect channel position after index file reads: " +
          s"expected $expectedPosition but actual position was $actualPosition.")
      }
      // 构造并返回FileSegmentManagedBuffer
      new FileSegmentManagedBuffer(
        transportConf,
        getDataFile(blockId.shuffleId, blockId.mapId),
        // 读取的起始偏移量为offset
        offset,
        // 读取长度为nextOffset - offset
        nextOffset - offset)
    } finally {
      in.close()
    }
  }

  override def stop(): Unit = {}
}

private[spark] object IndexShuffleBlockResolver {
  // No-op reduce ID used in interactions with disk store.
  // The disk store currently expects puts to relate to a (map, reduce) pair, but in the sort
  // shuffle outputs for several reduces are glommed into a single file.
  val NOOP_REDUCE_ID = 0
}
