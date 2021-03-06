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

package org.apache.spark.storage.memory

import java.io.OutputStream
import java.nio.ByteBuffer
import java.util.LinkedHashMap

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import com.google.common.io.ByteStreams

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{UNROLL_MEMORY_CHECK_PERIOD, UNROLL_MEMORY_GROWTH_FACTOR}
import org.apache.spark.memory.{MemoryManager, MemoryMode}
import org.apache.spark.serializer.{SerializationStream, SerializerManager}
import org.apache.spark.storage._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.util.{SizeEstimator, Utils}
import org.apache.spark.util.collection.SizeTrackingVector
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

/**
 * MemoryStore负责将Block存储到内存。Spark通过将广播数据、RDD、Shuffe数据存储到内存，减少了对磁盘I/O的依赖，提高了程序的读写效率
 * Spark将内存中的Block抽象为特质MemoryEntry
 *
 * @tparam T
 */
private sealed trait MemoryEntry[T] {
  def size: Long // 当前block的大小
  def memoryMode: MemoryMode // Block存入内存的模式
  def classTag: ClassTag[T] // Block的类型标记
}
// DeserializedMemoryEntry表示反序列化后的MemoryEntry
private case class DeserializedMemoryEntry[T](
    value: Array[T],
    size: Long,
    classTag: ClassTag[T]) extends MemoryEntry[T] {
  val memoryMode: MemoryMode = MemoryMode.ON_HEAP
}

// SerializedMemoryEntry表示序列化后的MemoryEntry。
private case class SerializedMemoryEntry[T](
    buffer: ChunkedByteBuffer,
    memoryMode: MemoryMode,
    classTag: ClassTag[T]) extends MemoryEntry[T] {
  def size: Long = buffer.size
}

/**
 * RDD 在缓存到内存之前，Partition 中的数据一般以迭代器( Iterator )的数据结构来访问，
 * 通过 Iterator 可以获得分区中每一条序列化或者非序列化的 Record，这些Record在访问的时候占用的是 JVM 堆内存中 other 部分的内存区域，
 * 同一个Partition 的不同 Record 的空间并不是连续的。RDD 被缓存之后，会由 Partition 转化为 Block，并且存储位置变为了 Storage Memory 区域，
 * 此时 Block 中的 Record 所占用的内存空间是连续的。Unroll 意思是展开，在 Spark 当中的意义就是将存储在 Partition 中的 Record 由
 * 不连续的存储空间转换为连续存储空间的过程。Unroll 操作的时候需要在 Storage Memory 当中通过reserveUnrollMemoryForThisTask来申请
 * Unroll 操作所需要的内存，使用完毕之后，又通过releaseUnrollMemoryForThisTask方法来释放这部分内存。因为不能保证存储空间可以一次容纳 Iterator
 * 中的所有数据，当前的计算任务在 Unroll 时要向 MemoryManager 申请足够的 Unroll 空间来临时占位，空间不足则 Unroll 失败，空间足够时可以继续进行。
 * Unroll 并不是一下子把数据展开到内存，而是分布进行，在每步中都先检查内存是否足够，如果内存不足，则尝试将内存中的数据写入磁盘，释放空间存放新写入的数据，
 * 当计算释放空间足够时，则把内存中释放的数据写入到磁盘并返回内存足够的结果，而当计算出释放所有空间都不足时，则返回内存不足的结果。
 */
private[storage] trait BlockEvictionHandler {
  /**
   *
   *
   * Drop a block from memory, possibly putting it on disk if applicable. Called when the memory
   * store reaches its limit and needs to free up space.
   *
   * If `data` is not put on disk, it won't be created.
   *
   * The caller of this method must hold a write lock on the block before calling this method.
   * This method does not release the write lock.
   *
   * @return the block's new effective StorageLevel.
   */
  private[storage] def dropFromMemory[T: ClassTag](
      blockId: BlockId,
      data: () => Either[Array[T], ChunkedByteBuffer]): StorageLevel
}

/**
 * 在内存中存储blocks(块)，可能是java的序列化对象也可能是序列化的字节缓存
 * Stores blocks in memory, either as Arrays of deserialized Java objects or as
 * serialized ByteBuffers.
 */
private[spark] class MemoryStore(
    conf: SparkConf,
    blockInfoManager: BlockInfoManager, // Block信息管理器BlockInfoManager。
    serializerManager: SerializerManager, // 即序列化管理器SerializerManager。
//    即内存管理器MemoryManager。MemoryStore存储Block，使用的就是MemoryManager内的maxOnHeapStorageMemory和maxOffHeapStorageMemory两块内存池。
    memoryManager: MemoryManager,
//    Block驱逐处理器。blockEvictionHandler用于将Block从内存中驱逐出去。blockEvictionHandler的类型是BlockEvictionHandler, BlockEvictionHandler定义了将对象从内存中移除的接口
    blockEvictionHandler: BlockEvictionHandler)
  extends Logging {

  /**
   * 注意：对内存分配的所有更改，特别是放入块、驱逐块以及获取或释放展开内存，都必须在 `memoryManager` 上同步！
   */
  // Note: all changes to memory allocations, notably putting blocks, evicting blocks, and
  // acquiring or releasing unroll memory, must be synchronized on `memoryManager`!
// 内存中的BlockId与MemoryEntry（Block的内存形式）之间映射关系的缓存。
  private val entries = new LinkedHashMap[BlockId, MemoryEntry[_]](32, 0.75f, true)

  // taskAttemptId 表示申请内存的 task id
  // A mapping from taskAttemptId to amount of memory used for  a bunrollinglock (in bytes)
  // All accesses of this map are assumed to have manually synchronized on `memoryManager`
  // 堆内内存，key为申请内存的task id,value为申请的字节数大小
  /**
   * 任务尝试线程的标识TaskAttemptId与任务尝试线程在堆内存展开的所有Block占用的内存大小之和之间的映射关系。
   */
  private val onHeapUnrollMemoryMap = mutable.HashMap[Long, Long]()

  // 注意：堆外展开内存仅在 putIteratorAsBytes() 中使用，因为堆外缓存始终存储序列化值。
  // Note: off-heap unroll memory is only used in putIteratorAsBytes() because off-heap caching
  // always stores serialized values.
  /**
   * 任务尝试线程的标识TaskAttemptId与任务尝试线程在堆外内存展开的所有Block占用的内存大小之和之间的映射关系。
   */
  private val offHeapUnrollMemoryMap = mutable.HashMap[Long, Long]()

  // 用来展开任何Block之前，初始请求的内存大小，可以修改属性spark.storage.unrollMemoryThreshold（默认为1MB）改变大小。
  // Initial memory to request before unrolling any block
  private val unrollMemoryThreshold: Long =
    conf.getLong("spark.storage.unrollMemoryThreshold", 1024 * 1024)

  /** MemoryStore用于存储Block的最大内存，其实质为MemoryManager的maxOnHeapStorageMemory和maxOffHeapStorageMemory之和。
   * 如果Memory Manager为StaticMemoryManager，那么maxMemory的大小是固定的。如果MemoryManager为UnifiedMemoryManager，那么maxMemory的大小是动态变化的。
   * Total amount of memory available for storage, in bytes. */
  private def maxMemory: Long = {
    // 堆内缓存内存 + 堆外缓存内存
    memoryManager.maxOnHeapStorageMemory + memoryManager.maxOffHeapStorageMemory
  }

  if (maxMemory < unrollMemoryThreshold) {
    logWarning(s"Max memory ${Utils.bytesToString(maxMemory)} is less than the initial memory " +
      s"threshold ${Utils.bytesToString(unrollMemoryThreshold)} needed to store a block in " +
      s"memory. Please configure Spark with more memory.")
  }

  logInfo("MemoryStore started with capacity %s".format(Utils.bytesToString(maxMemory)))

  // 使用的总存储内存，包括unroll内存，以字节为单位
  /**
   * MemoryStore中已经使用的内存大小。其实质为MemoryManager中onHeapStorageMemoryPool已经使用的大小和offHeapStorageMemoryPool已经使用的大小之和。
   */
  /** Total storage memory used including unroll memory, in bytes. */
  private def memoryUsed: Long = memoryManager.storageMemoryUsed

  /**
   * 用于缓存块的存储内存量（以字节为单位）。这不包括用于untroll的内存。
   * Storage内存的作用是：缓存RDD和广播变量，主要是以缓存RDD为主
   *
   * MemoryStore用于存储Block（即MemoryEntry）使用的内存大小，即memoryUsed与currentUnrollMemory的差值。
   *
   * Amount of storage memory, in bytes, used for caching blocks.
   * This does not include memory used for unrolling.
   */
  private def blocksMemoryUsed: Long = memoryManager.synchronized {
    // Storage - 已经展开的内存，剩下的就是可以用于缓存block的内存
    memoryUsed - currentUnrollMemory
  }

  def getSize(blockId: BlockId): Long = {
    entries.synchronized {
      entries.get(blockId).size
    }
  }

  /**
   * 使用 size 来测试 MemoryStore 中是否有足够的空间。如果是，则创建 ByteBuffer 并将其放入 MemoryStore。否则，将不会创建 ByteBuffer。
     调用者应保证大小正确。
     返回：
     如果 put() 成功，则为 true，否则为 false。
   * Use `size` to test if there is enough space in MemoryStore. If so, create the ByteBuffer and
   * put it into MemoryStore. Otherwise, the ByteBuffer won't be created.
   *
   * The caller should guarantee that `size` is correct.
   *
   * @return true if the put() succeeded, false otherwise.
   */
  def putBytes[T: ClassTag](
      blockId: BlockId,
      size: Long,
      memoryMode: MemoryMode,
      _bytes: () => ChunkedByteBuffer): Boolean = {
    require(!contains(blockId), s"Block $blockId is already present in the MemoryStore")
    if (memoryManager.acquireStorageMemory(blockId, size, memoryMode)) { // 获取逻辑内存
      // We acquired enough memory for the block, so go ahead and put it
      val bytes = _bytes() // 获取Block数据，即ChunkedByteBuffer。
      assert(bytes.size == size)
      val entry = new SerializedMemoryEntry[T](bytes, memoryMode, implicitly[ClassTag[T]])
      entries.synchronized {
        entries.put(blockId, entry) // 将Block数据写入内存
      }
      logInfo("Block %s stored as bytes in memory (estimated size %s, free %s)".format(
        blockId, Utils.bytesToString(size), Utils.bytesToString(maxMemory - blocksMemoryUsed)))
      true
    } else {
      false
    }
  }

  /**
   * 尝试将给定的块作为值或字节放入内存中。
     迭代器可能太大而无法在内存中实现和存储。为了避免OOM异常，这个方法会在周期性的检查是否有足够的空闲内存的同时，逐步展开迭代器。
     如果块成功实现，那么在实现过程中使用的临时展开内存将“转移”到存储内存，因此我们不会获得比存储块实际所需的更多内存。

   * Attempt to put the given block in memory store as values or bytes.
   *
   * It's possible that the iterator is too large to materialize and store in memory. To avoid
   * OOM exceptions, this method will gradually unroll the iterator while periodically checking
   * whether there is enough free memory. If the block is successfully materialized, then the
   * temporary unroll memory used during the materialization is "transferred" to storage memory,
   * so we won't acquire more memory than is actually needed to store the block.
   *
   * @param blockId The block id.
   * @param values The values which need be stored.
   * @param classTag the [[ClassTag]] for the block.
   * @param memoryMode The values saved memory mode(ON_HEAP or OFF_HEAP).
   * @param valuesHolder A holder that supports storing record of values into memory store as
   *        values or bytes.
   * @return if the block is stored successfully, return the stored data size. Else return the
   *         memory has reserved for unrolling the block (There are two reasons for store failed:
   *         First, the block is partially-unrolled; second, the block is entirely unrolled and
   *         the actual stored data size is larger than reserved, but we can't request extra
   *         memory).
   */
  private def putIterator[T](
      blockId: BlockId,
      values: Iterator[T],
      classTag: ClassTag[T],
      memoryMode: MemoryMode,
      valuesHolder: ValuesHolder[T]): Either[Long, Long] = {
    require(!contains(blockId), s"Block $blockId is already present in the MemoryStore")

    // Number of elements unrolled so far 目前为止，到目前为止展开的unrolled的元素数
    var elementsUnrolled = 0
    // Whether there is still enough memory for us to continue unrolling this block 是否还有足够的内存让我们继续展开这个块
    var keepUnrolling = true
    // Initial per-task memory to request for unrolling blocks (bytes). 用于请求展开块（字节）的初始每任务内存。
    val initialMemoryThreshold = unrollMemoryThreshold
    // How often to check whether we need to request more memory。 检查我们是否需要请求更多内存的频率
    val memoryCheckPeriod = conf.get(UNROLL_MEMORY_CHECK_PERIOD)
    // Memory currently reserved by this task for this particular unrolling operation  此任务当前为此特定untroll操作保留的内存
    var memoryThreshold = initialMemoryThreshold
    // Memory to request as a multiple of current vector size 作为当前向量大小的倍数请求的内存
    val memoryGrowthFactor = conf.get(UNROLL_MEMORY_GROWTH_FACTOR)
    // Keep track of unroll memory used by this particular block / putIterator() operation  跟踪此特定块 / putIterator() 操作使用的展开内存
    var unrollMemoryUsedByThisBlock = 0L

    // Request enough memory to begin unrolling  请求足够的内存以开始展开
    keepUnrolling =
      reserveUnrollMemoryForThisTask(blockId, initialMemoryThreshold, memoryMode)

    if (!keepUnrolling) {
      logWarning(s"Failed to reserve initial memory threshold of " +
        s"${Utils.bytesToString(initialMemoryThreshold)} for computing block $blockId in memory.")
    } else {
      unrollMemoryUsedByThisBlock += initialMemoryThreshold
    }

    // 安全地展开这个块，定期检查我们是否超过了我们的阈值
    // Unroll this block safely, checking whether we have exceeded our threshold periodically
    while (values.hasNext && keepUnrolling) {
      valuesHolder.storeValue(values.next())

      if (elementsUnrolled % memoryCheckPeriod == 0) {
        val currentSize = valuesHolder.estimatedSize()
        // If our vector's size has exceeded the threshold, request more memory
        if (currentSize >= memoryThreshold) {
          val amountToRequest = (currentSize * memoryGrowthFactor - memoryThreshold).toLong
          keepUnrolling =
            reserveUnrollMemoryForThisTask(blockId, amountToRequest, memoryMode)
          if (keepUnrolling) {
            unrollMemoryUsedByThisBlock += amountToRequest
          }
          // New threshold is currentSize * memoryGrowthFactor
          memoryThreshold += amountToRequest
        }
      }
      elementsUnrolled += 1
    }

    // Make sure that we have enough memory to store the block. By this point, it is possible that
    // the block's actual memory usage has exceeded the unroll memory by a small amount, so we
    // perform one final call to attempt to allocate additional memory if necessary.
    if (keepUnrolling) {
      val entryBuilder = valuesHolder.getBuilder()
      val size = entryBuilder.preciseSize
      if (size > unrollMemoryUsedByThisBlock) {
        val amountToRequest = size - unrollMemoryUsedByThisBlock
        keepUnrolling = reserveUnrollMemoryForThisTask(blockId, amountToRequest, memoryMode)
        if (keepUnrolling) {
          unrollMemoryUsedByThisBlock += amountToRequest
        }
      }

      if (keepUnrolling) {
        val entry = entryBuilder.build()
        // Synchronize so that transfer is atomic
        memoryManager.synchronized {
          releaseUnrollMemoryForThisTask(memoryMode, unrollMemoryUsedByThisBlock)
          val success = memoryManager.acquireStorageMemory(blockId, entry.size, memoryMode)
          assert(success, "transferring unroll memory to storage memory failed")
        }

        entries.synchronized {
          entries.put(blockId, entry)
        }

        logInfo("Block %s stored as values in memory (estimated size %s, free %s)".format(blockId,
          Utils.bytesToString(entry.size), Utils.bytesToString(maxMemory - blocksMemoryUsed)))
        Right(entry.size)
      } else {
        // We ran out of space while unrolling the values for this block
        logUnrollFailureMessage(blockId, entryBuilder.preciseSize)
        Left(unrollMemoryUsedByThisBlock)
      }
    } else {
      // We ran out of space while unrolling the values for this block
      logUnrollFailureMessage(blockId, valuesHolder.estimatedSize())
      Left(unrollMemoryUsedByThisBlock)
    }
  }

  /**
   * 此方法将BlockId对应的Block（已经转换为Iterator）写入内存。有时候放入内存的Block很大，所以一次性将此对象写入内存可能将引发OOM
   * （即java.lang.OutOfMemoryError）异常。为了避免这种情况的发生，首先需要将Block转换为Iterator，然后渐进式地展开此Iterator，
   * 并且周期性地检查是否有足够的展开内存。此方法涉及很多变量，为了便于理解，这里先解释这些变量的含义，然后再分析方法实现。
   *
   * Attempt to put the given block in memory store as values.
   *
   * @return in case of success, the estimated size of the stored data. In case of failure, return
   *         an iterator containing the values of the block. The returned iterator will be backed
   *         by the combination of the partially-unrolled block and the remaining elements of the
   *         original input iterator. The caller must either fully consume this iterator or call
   *         `close()` on it in order to free the storage memory consumed by the partially-unrolled
   *         block.
   */
  private[storage] def putIteratorAsValues[T](
      blockId: BlockId,
      values: Iterator[T],
      classTag: ClassTag[T]): Either[PartiallyUnrolledIterator[T], Long] = {

    val valuesHolder = new DeserializedValuesHolder[T](classTag)

    putIterator(blockId, values, classTag, MemoryMode.ON_HEAP, valuesHolder) match {
      case Right(storedSize) => Right(storedSize)
      case Left(unrollMemoryUsedByThisBlock) =>
        val unrolledIterator = if (valuesHolder.vector != null) {
          valuesHolder.vector.iterator
        } else {
          valuesHolder.arrayValues.toIterator
        }

        Left(new PartiallyUnrolledIterator(
          this,
          MemoryMode.ON_HEAP,
          unrollMemoryUsedByThisBlock,
          unrolled = unrolledIterator,
          rest = values))
    }
  }

  /**
   * Attempt to put the given block in memory store as bytes.
   *
   * @return in case of success, the estimated size of the stored data. In case of failure,
   *         return a handle which allows the caller to either finish the serialization by
   *         spilling to disk or to deserialize the partially-serialized block and reconstruct
   *         the original input iterator. The caller must either fully consume this result
   *         iterator or call `discard()` on it in order to free the storage memory consumed by the
   *         partially-unrolled block.
   */
  private[storage] def putIteratorAsBytes[T](
      blockId: BlockId,
      values: Iterator[T],
      classTag: ClassTag[T],
      memoryMode: MemoryMode): Either[PartiallySerializedBlock[T], Long] = {

    require(!contains(blockId), s"Block $blockId is already present in the MemoryStore")

    // Initial per-task memory to request for unrolling blocks (bytes).
    val initialMemoryThreshold = unrollMemoryThreshold
    val chunkSize = if (initialMemoryThreshold > ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH) {
      logWarning(s"Initial memory threshold of ${Utils.bytesToString(initialMemoryThreshold)} " +
        s"is too large to be set as chunk size. Chunk size has been capped to " +
        s"${Utils.bytesToString(ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH)}")
      ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH
    } else {
      initialMemoryThreshold.toInt
    }

    val valuesHolder = new SerializedValuesHolder[T](blockId, chunkSize, classTag,
      memoryMode, serializerManager)

    putIterator(blockId, values, classTag, memoryMode, valuesHolder) match {
      case Right(storedSize) => Right(storedSize)
      case Left(unrollMemoryUsedByThisBlock) =>
        Left(new PartiallySerializedBlock(
          this,
          serializerManager,
          blockId,
          valuesHolder.serializationStream,
          valuesHolder.redirectableStream,
          unrollMemoryUsedByThisBlock,
          memoryMode,
          valuesHolder.bbos,
          values,
          classTag))
    }
  }

  /**
   * 此方法从内存中读取BlockId对应的Block（已经封装为ChunkedByteBuffer）
   *
   * @param blockId
   * @return
   */
  def getBytes(blockId: BlockId): Option[ChunkedByteBuffer] = {
    val entry = entries.synchronized { entries.get(blockId) }
    entry match {
      case null => None
      case e: DeserializedMemoryEntry[_] =>
        throw new IllegalArgumentException("should only call getBytes on serialized blocks")
      case SerializedMemoryEntry(bytes, _, _) => Some(bytes)
    }
  }

  /**
   * 此方法用于从内存中读取BlockId对应的Block（已经封装为Iterator）
   *
   * @param blockId
   * @return
   */
  def getValues(blockId: BlockId): Option[Iterator[_]] = {
    val entry = entries.synchronized { entries.get(blockId) }
    entry match {
      case null => None
      case e: SerializedMemoryEntry[_] =>
        throw new IllegalArgumentException("should only call getValues on deserialized blocks")
      case DeserializedMemoryEntry(values, _, _) =>
        val x = Some(values)
        x.map(_.iterator)
    }
  }

  /**
   * 此方法用于从内存中移除BlockId对应的Block
   *
   * @param blockId
   * @return
   */
  def remove(blockId: BlockId): Boolean = memoryManager.synchronized {
    val entry = entries.synchronized {
      entries.remove(blockId) // 将Block从内存移除
    }
    if (entry != null) {
      entry match {
        case SerializedMemoryEntry(buffer, _, _) => buffer.dispose()
        case _ =>
      }
      // 释放逻辑的存储内存
      memoryManager.releaseStorageMemory(entry.size, entry.memoryMode)
      logDebug(s"Block $blockId of size ${entry.size} dropped " +
        s"from memory (free ${maxMemory - blocksMemoryUsed})")
      true // 移除成功
    } else {
      false // 移除失败
    }
  }

  def clear(): Unit = memoryManager.synchronized {
    entries.synchronized {
      entries.clear()
    }
    onHeapUnrollMemoryMap.clear()
    offHeapUnrollMemoryMap.clear()
    memoryManager.releaseAllStorageMemory()
    logInfo("MemoryStore cleared")
  }

  /**
   * 通过block Id返回所属的RDD,若不是Rdd的block,则返回None
   * Return the RDD ID that a given block ID is from, or None if it is not an RDD block.
   */
  private def getRddId(blockId: BlockId): Option[Int] = {
    blockId.asRDDId.map(_.rddId)
  }

  /**
   * 此方法用于驱逐Block，以便释放一些空间来存储新的Block
   *
   * Try to evict blocks to free up a given amount of space to store a particular block.
   * Can fail if either the block is bigger than our memory or it would require replacing
   * another block from the same RDD (which leads to a wasteful cyclic replacement pattern for
   * RDDs that don't fit into memory that we want to avoid).
   *
   * @param blockId the ID of the block we are freeing space for, if any
   * @param space the size of this block  块的大小
   * @param memoryMode the type of memory to free (on- or off-heap)
   * @return the amount of memory (in bytes) freed by eviction
   */
  private[spark] def evictBlocksToFreeSpace(
      blockId: Option[BlockId], // 要存储的Block的BlockId。
      space: Long, // 需要驱逐Block所腾出的内存大小。
      memoryMode: MemoryMode): Long = { // 存储Block所需的内存模式。
    assert(space > 0)
    memoryManager.synchronized {
      var freedMemory = 0L // 已经释放的内存大小。
      // 需要添加的RDD
      val rddToAdd = blockId.flatMap(getRddId) // 将要添加的RDD的RDDBlockId标记。rddToAdd实际是通过对BlockId应用getRddId方法得到的。
      // 选择的块
      val selectedBlocks = new ArrayBuffer[BlockId]
      // 判断块是否是可驱逐的RDD
      def blockIsEvictable(blockId: BlockId, entry: MemoryEntry[_]): Boolean = {
        // 存储模式相同，且blockId没有被RDD占用，或不是要替换相同RDD的不同数据块
        entry.memoryMode == memoryMode && (rddToAdd.isEmpty || rddToAdd != getRddId(blockId))
      }
      // This is synchronized to ensure that the set of entries is not changed
      // (because of getValue or getBytes) while traversing the iterator, as that
      // can lead to exceptions.
      entries.synchronized {
        // 遍历内存
        val iterator = entries.entrySet().iterator()
        // 当freedMemory小于space时，不断迭代遍历iterator。对于每个entries中的BlockId和MemoryEntry，
        // 首先找出其中符合条件的Block（只有符合条件的Block才会被驱逐），然后获取Block的写锁，
        // 最后将此Block的BlockId放入selectedBlocks并且将freedMemory增加Block的大小
        while (freedMemory < space && iterator.hasNext) { // 选择符合驱逐条件的Block
          val pair = iterator.next()
          // 获取blockId
          val blockId = pair.getKey
          // 获取block对应的内存空间
          val entry = pair.getValue
          // 若可以驱逐
          if (blockIsEvictable(blockId, entry)) {
            // We don't want to evict blocks which are currently being read, so we need to obtain
            // an exclusive write lock on blocks which are candidates for eviction. We perform a
            // non-blocking "tryLock" here in order to ignore blocks which are locked for reading:
            /**
             * 我们不限驱逐正在被读取的块，所以我们需要在用来驱逐的候选人块获取一个排它的写锁。
             * 我们在这里执行一个非阻塞的tryLock为了忽略被锁定的读的块
             */
            if (blockInfoManager.lockForWriting(blockId, blocking = false).isDefined) {
              selectedBlocks += blockId
              freedMemory += pair.getValue.size
            }
          }
        }
      }

      // 驱逐块
      def dropBlock[T](blockId: BlockId, entry: MemoryEntry[T]): Unit = {
        val data = entry match {
          // 饭序列化的内存Entry
          case DeserializedMemoryEntry(values, _, _) => Left(values)
          // 序列化的内存Entry
          case SerializedMemoryEntry(buffer, _, _) => Right(buffer)
        }
        val newEffectiveStorageLevel =
          blockEvictionHandler.dropFromMemory(blockId, () => data)(entry.classTag)
        if (newEffectiveStorageLevel.isValid) {
          // The block is still present in at least one store, so release the lock
          // but don't delete the block info
          blockInfoManager.unlock(blockId)
        } else {
          // The block isn't present in any store, so delete the block info so that the
          // block can be stored again
          blockInfoManager.removeBlock(blockId)
        }
      }

      if (freedMemory >= space) {
        var lastSuccessfulBlock = -1
        try {
          logInfo(s"${selectedBlocks.size} blocks selected for dropping " +
            s"(${Utils.bytesToString(freedMemory)} bytes)")
          (0 until selectedBlocks.size).foreach { idx =>
            val blockId = selectedBlocks(idx)
            val entry = entries.synchronized {
              entries.get(blockId)
            }
            // This should never be null as only one task should be dropping
            // blocks and removing entries. However the check is still here for
            // future safety.
            if (entry != null) {
              dropBlock(blockId, entry)
              afterDropAction(blockId)
            }
            lastSuccessfulBlock = idx
          }
          logInfo(s"After dropping ${selectedBlocks.size} blocks, " +
            s"free memory is ${Utils.bytesToString(maxMemory - blocksMemoryUsed)}")
          freedMemory
        } finally {
          // like BlockManager.doPut, we use a finally rather than a catch to avoid having to deal
          // with InterruptedException
          if (lastSuccessfulBlock != selectedBlocks.size - 1) {
            // the blocks we didn't process successfully are still locked, so we have to unlock them
            (lastSuccessfulBlock + 1 until selectedBlocks.size).foreach { idx =>
              val blockId = selectedBlocks(idx)
              blockInfoManager.unlock(blockId)
            }
          }
        }
      } else {
        blockId.foreach { id =>
          logInfo(s"Will not store $id")
        }
        selectedBlocks.foreach { id =>
          blockInfoManager.unlock(id)
        }
        0L
      }
    }
  }

  // hook for testing, so we can simulate a race
  protected def afterDropAction(blockId: BlockId): Unit = {}

  def contains(blockId: BlockId): Boolean = {
    entries.synchronized { entries.containsKey(blockId) }
  }

  private def currentTaskAttemptId(): Long = {
    // In case this is called on the driver, return an invalid task attempt id.
    Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(-1L)
  }

  /**
   * 此方法用于为展开尝试执行任务给定的Block，保留指定内存模式上指定大小的内存
   *
   * 保留内存用于展开此任务的给定块。
   * Reserve memory for unrolling the given block for this task.
   *
   * @return whether the request is granted.
   */
  def reserveUnrollMemoryForThisTask(
      blockId: BlockId,
      memory: Long,
      memoryMode: MemoryMode): Boolean = {
    memoryManager.synchronized {
      // 获取内存，必要时驱逐块。调用MemoryManager的acquireUnrollMemory方法获取展开内存。
      val success = memoryManager.acquireUnrollMemory(blockId, memory, memoryMode)
      // 若成功
      if (success) {
        // 则更新taskAttemptId与任务尝试线程在堆内存或堆外内存展开的所有Block占用的内存大小之和之间的映射关系。
        val taskAttemptId = currentTaskAttemptId()

        val unrollMemoryMap = memoryMode match {
          case MemoryMode.ON_HEAP => onHeapUnrollMemoryMap
          case MemoryMode.OFF_HEAP => offHeapUnrollMemoryMap
        }
        unrollMemoryMap(taskAttemptId) = unrollMemoryMap.getOrElse(taskAttemptId, 0L) + memory
      }
      success // 返回成功或失败的状态
    }
  }

  /**
   * 此方法用于释放任务尝试线程占用的内存
   *
   * Release memory used by this task for unrolling blocks.
   * If the amount is not specified, remove the current task's allocation altogether.
   */
  def releaseUnrollMemoryForThisTask(memoryMode: MemoryMode, memory: Long = Long.MaxValue): Unit = {
    val taskAttemptId = currentTaskAttemptId()
    memoryManager.synchronized {
      val unrollMemoryMap = memoryMode match {
        case MemoryMode.ON_HEAP => onHeapUnrollMemoryMap
        case MemoryMode.OFF_HEAP => offHeapUnrollMemoryMap
      }
      if (unrollMemoryMap.contains(taskAttemptId)) { // 计算要释放的内存
        val memoryToRelease = math.min(memory, unrollMemoryMap(taskAttemptId))
        if (memoryToRelease > 0) { // 释放展开内存
          unrollMemoryMap(taskAttemptId) -= memoryToRelease
          memoryManager.releaseUnrollMemory(memoryToRelease, memoryMode)
        }
        if (unrollMemoryMap(taskAttemptId) == 0) {
          unrollMemoryMap.remove(taskAttemptId) // 清除taskAttemptId与展开内存大小之间的映射关系
        }
      }
    }
  }

  /**
   *
   * MemoryStore用于展开Block使用的内存大小。
   * 其实质为onHeapUnrollMemoryMap中的所有用于展开Block所占用的内存大小与offHeapUnrollMemoryMap中的所有用于展开Block所占用的内存大小之和。
   *
   * Return the amount of memory currently occupied for unrolling blocks across all tasks.
   */
  def currentUnrollMemory: Long = memoryManager.synchronized {
    onHeapUnrollMemoryMap.values.sum + offHeapUnrollMemoryMap.values.sum
  }

  /**
   * 当前的任务尝试线程用于展开Block所占用的内存。
   * 即onHeapUnrollMemoryMap中缓存的当前任务尝试线程对应的占用大小与offHeapUnrollMemoryMap中缓存的当前的任务尝试线程对应的占
   *
   * Return the amount of memory currently occupied for unrolling blocks by this task.
   */
  def currentUnrollMemoryForThisTask: Long = memoryManager.synchronized {
    onHeapUnrollMemoryMap.getOrElse(currentTaskAttemptId(), 0L) +
      offHeapUnrollMemoryMap.getOrElse(currentTaskAttemptId(), 0L)
  }

  /**
   * 当前使用MemoryStore展开Block的任务的数量。其实质为onHeapUnrollMemoryMap的键集合与offHeapUnrollMemoryMap的键集合的并集。
   *
   * Return the number of tasks currently unrolling blocks.
   */
  private def numTasksUnrolling: Int = memoryManager.synchronized {
    (onHeapUnrollMemoryMap.keys ++ offHeapUnrollMemoryMap.keys).toSet.size
  }

  /**
   * Log information about current memory usage.
   */
  private def logMemoryUsage(): Unit = {
    logInfo(
      s"Memory use = ${Utils.bytesToString(blocksMemoryUsed)} (blocks) + " +
      s"${Utils.bytesToString(currentUnrollMemory)} (scratch space shared across " +
      s"$numTasksUnrolling tasks(s)) = ${Utils.bytesToString(memoryUsed)}. " +
      s"Storage limit = ${Utils.bytesToString(maxMemory)}."
    )
  }

  /**
   * Log a warning for failing to unroll a block.
   *
   * @param blockId ID of the block we are trying to unroll.
   * @param finalVectorSize Final size of the vector before unrolling failed.
   */
  private def logUnrollFailureMessage(blockId: BlockId, finalVectorSize: Long): Unit = {
    logWarning(
      s"Not enough space to cache $blockId in memory! " +
      s"(computed ${Utils.bytesToString(finalVectorSize)} so far)"
    )
    logMemoryUsage()
  }
}

private trait MemoryEntryBuilder[T] {
  def preciseSize: Long
  def build(): MemoryEntry[T]
}

private trait ValuesHolder[T] {
  def storeValue(value: T): Unit
  def estimatedSize(): Long

  /**
   * Note: After this method is called, the ValuesHolder is invalid, we can't store data and
   * get estimate size again.
   * @return a MemoryEntryBuilder which is used to build a memory entry and get the stored data
   *         size.
   */
  def getBuilder(): MemoryEntryBuilder[T]
}

/**
 * A holder for storing the deserialized values.
 */
private class DeserializedValuesHolder[T] (classTag: ClassTag[T]) extends ValuesHolder[T] {
  // Underlying vector for unrolling the block
  var vector = new SizeTrackingVector[T]()(classTag)
  var arrayValues: Array[T] = null

  override def storeValue(value: T): Unit = {
    vector += value
  }

  override def estimatedSize(): Long = {
    vector.estimateSize()
  }

  override def getBuilder(): MemoryEntryBuilder[T] = new MemoryEntryBuilder[T] {
    // We successfully unrolled the entirety of this block
    arrayValues = vector.toArray
    vector = null

    override val preciseSize: Long = SizeEstimator.estimate(arrayValues)

    override def build(): MemoryEntry[T] =
      DeserializedMemoryEntry[T](arrayValues, preciseSize, classTag)
  }
}

/**
 * A holder for storing the serialized values.
 */
private class SerializedValuesHolder[T](
    blockId: BlockId,
    chunkSize: Int,
    classTag: ClassTag[T],
    memoryMode: MemoryMode,
    serializerManager: SerializerManager) extends ValuesHolder[T] {
  val allocator = memoryMode match {
    case MemoryMode.ON_HEAP => ByteBuffer.allocate _
    case MemoryMode.OFF_HEAP => Platform.allocateDirectBuffer _
  }

  val redirectableStream = new RedirectableOutputStream
  val bbos = new ChunkedByteBufferOutputStream(chunkSize, allocator)
  redirectableStream.setOutputStream(bbos)
  val serializationStream: SerializationStream = {
    val autoPick = !blockId.isInstanceOf[StreamBlockId]
    val ser = serializerManager.getSerializer(classTag, autoPick).newInstance()
    ser.serializeStream(serializerManager.wrapForCompression(blockId, redirectableStream))
  }

  override def storeValue(value: T): Unit = {
    serializationStream.writeObject(value)(classTag)
  }

  override def estimatedSize(): Long = {
    bbos.size
  }

  override def getBuilder(): MemoryEntryBuilder[T] = new MemoryEntryBuilder[T] {
    // We successfully unrolled the entirety of this block
    serializationStream.close()

    override def preciseSize(): Long = bbos.size

    override def build(): MemoryEntry[T] =
      SerializedMemoryEntry[T](bbos.toChunkedByteBuffer, memoryMode, classTag)
  }
}

/**
 * The result of a failed [[MemoryStore.putIteratorAsValues()]] call.
 *
 * @param memoryStore  the memoryStore, used for freeing memory.
 * @param memoryMode   the memory mode (on- or off-heap).
 * @param unrollMemory the amount of unroll memory used by the values in `unrolled`.
 * @param unrolled     an iterator for the partially-unrolled values.
 * @param rest         the rest of the original iterator passed to
 *                     [[MemoryStore.putIteratorAsValues()]].
 */
private[storage] class PartiallyUnrolledIterator[T](
    memoryStore: MemoryStore,
    memoryMode: MemoryMode,
    unrollMemory: Long,
    private[this] var unrolled: Iterator[T],
    rest: Iterator[T])
  extends Iterator[T] {

  private def releaseUnrollMemory(): Unit = {
    memoryStore.releaseUnrollMemoryForThisTask(memoryMode, unrollMemory)
    // SPARK-17503: Garbage collects the unrolling memory before the life end of
    // PartiallyUnrolledIterator.
    unrolled = null
  }

  override def hasNext: Boolean = {
    if (unrolled == null) {
      rest.hasNext
    } else if (!unrolled.hasNext) {
      releaseUnrollMemory()
      rest.hasNext
    } else {
      true
    }
  }

  override def next(): T = {
    if (unrolled == null || !unrolled.hasNext) {
      rest.next()
    } else {
      unrolled.next()
    }
  }

  /**
   * Called to dispose of this iterator and free its memory.
   */
  def close(): Unit = {
    if (unrolled != null) {
      releaseUnrollMemory()
    }
  }
}

/**
 * 即这个类可以将outputstream重定向到另一个outputstream。
 * A wrapper which allows an open [[OutputStream]] to be redirected to a different sink.
 */
private[storage] class RedirectableOutputStream extends OutputStream {
  private[this] var os: OutputStream = _
  def setOutputStream(s: OutputStream): Unit = { os = s }
  override def write(b: Int): Unit = os.write(b)
  override def write(b: Array[Byte]): Unit = os.write(b)
  override def write(b: Array[Byte], off: Int, len: Int): Unit = os.write(b, off, len)
  override def flush(): Unit = os.flush()
  override def close(): Unit = os.close()
}

/**
 * The result of a failed [[MemoryStore.putIteratorAsBytes()]] call.
 *
 * @param memoryStore the MemoryStore, used for freeing memory.
 * @param serializerManager the SerializerManager, used for deserializing values.
 * @param blockId the block id.
 * @param serializationStream a serialization stream which writes to [[redirectableOutputStream]].
 * @param redirectableOutputStream an OutputStream which can be redirected to a different sink.
 * @param unrollMemory the amount of unroll memory used by the values in `unrolled`.
 * @param memoryMode whether the unroll memory is on- or off-heap
 * @param bbos byte buffer output stream containing the partially-serialized values.
 *                     [[redirectableOutputStream]] initially points to this output stream.
 * @param rest         the rest of the original iterator passed to
 *                     [[MemoryStore.putIteratorAsValues()]].
 * @param classTag the [[ClassTag]] for the block.
 */
private[storage] class PartiallySerializedBlock[T](
    memoryStore: MemoryStore,
    serializerManager: SerializerManager,
    blockId: BlockId,
    private val serializationStream: SerializationStream,
    private val redirectableOutputStream: RedirectableOutputStream,
    val unrollMemory: Long,
    memoryMode: MemoryMode,
    bbos: ChunkedByteBufferOutputStream,
    rest: Iterator[T],
    classTag: ClassTag[T]) {

  private lazy val unrolledBuffer: ChunkedByteBuffer = {
    bbos.close()
    bbos.toChunkedByteBuffer
  }

  // If the task does not fully consume `valuesIterator` or otherwise fails to consume or dispose of
  // this PartiallySerializedBlock then we risk leaking of direct buffers, so we use a task
  // completion listener here in order to ensure that `unrolled.dispose()` is called at least once.
  // The dispose() method is idempotent, so it's safe to call it unconditionally.
  Option(TaskContext.get()).foreach { taskContext =>
    taskContext.addTaskCompletionListener[Unit] { _ =>
      // When a task completes, its unroll memory will automatically be freed. Thus we do not call
      // releaseUnrollMemoryForThisTask() here because we want to avoid double-freeing.
      unrolledBuffer.dispose()
    }
  }

  // Exposed for testing
  private[storage] def getUnrolledChunkedByteBuffer: ChunkedByteBuffer = unrolledBuffer

  private[this] var discarded = false
  private[this] var consumed = false

  private def verifyNotConsumedAndNotDiscarded(): Unit = {
    if (consumed) {
      throw new IllegalStateException(
        "Can only call one of finishWritingToStream() or valuesIterator() and can only call once.")
    }
    if (discarded) {
      throw new IllegalStateException("Cannot call methods on a discarded PartiallySerializedBlock")
    }
  }

  /**
   * Called to dispose of this block and free its memory.
   */
  def discard(): Unit = {
    if (!discarded) {
      try {
        // We want to close the output stream in order to free any resources associated with the
        // serializer itself (such as Kryo's internal buffers). close() might cause data to be
        // written, so redirect the output stream to discard that data.
        redirectableOutputStream.setOutputStream(ByteStreams.nullOutputStream())
        serializationStream.close()
      } finally {
        discarded = true
        unrolledBuffer.dispose()
        memoryStore.releaseUnrollMemoryForThisTask(memoryMode, unrollMemory)
      }
    }
  }

  /**
   * Finish writing this block to the given output stream by first writing the serialized values
   * and then serializing the values from the original input iterator.
   */
  def finishWritingToStream(os: OutputStream): Unit = {
    verifyNotConsumedAndNotDiscarded()
    consumed = true
    // `unrolled`'s underlying buffers will be freed once this input stream is fully read:
    ByteStreams.copy(unrolledBuffer.toInputStream(dispose = true), os)
    memoryStore.releaseUnrollMemoryForThisTask(memoryMode, unrollMemory)
    redirectableOutputStream.setOutputStream(os)
    while (rest.hasNext) {
      serializationStream.writeObject(rest.next())(classTag)
    }
    serializationStream.close()
  }

  /**
   * Returns an iterator over the values in this block by first deserializing the serialized
   * values and then consuming the rest of the original input iterator.
   *
   * If the caller does not plan to fully consume the resulting iterator then they must call
   * `close()` on it to free its resources.
   */
  def valuesIterator: PartiallyUnrolledIterator[T] = {
    verifyNotConsumedAndNotDiscarded()
    consumed = true
    // Close the serialization stream so that the serializer's internal buffers are freed and any
    // "end-of-stream" markers can be written out so that `unrolled` is a valid serialized stream.
    serializationStream.close()
    // `unrolled`'s underlying buffers will be freed once this input stream is fully read:
    val unrolledIter = serializerManager.dataDeserializeStream(
      blockId, unrolledBuffer.toInputStream(dispose = true))(classTag)
    // The unroll memory will be freed once `unrolledIter` is fully consumed in
    // PartiallyUnrolledIterator. If the iterator is not consumed by the end of the task then any
    // extra unroll memory will automatically be freed by a `finally` block in `Task`.
    new PartiallyUnrolledIterator(
      memoryStore,
      memoryMode,
      unrollMemory,
      unrolled = unrolledIter,
      rest = rest)
  }
}
