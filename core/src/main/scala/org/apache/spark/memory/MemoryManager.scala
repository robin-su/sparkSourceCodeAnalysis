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

package org.apache.spark.memory

import javax.annotation.concurrent.GuardedBy

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.storage.BlockId
import org.apache.spark.storage.memory.MemoryStore
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.memory.MemoryAllocator

/**
 * 内存管理主要涉及了两个组件：JVM 范围的内存管理和单个任务的内存管理。
 * MemoryManager管理Spark在JVM中的总体内存使用情况。该组件实现了跨任务划分可用内存以及在存储（内存使用缓存和数据传输）
 * 和执行（计算使用的内存，如shuffle，连接，排序和聚合）之间分配内存的策略。
 *
 * An abstract memory manager that enforces how memory is shared between execution and storage.
 *
 * In this context, execution memory refers to that used for computation in shuffles, joins,
 * sorts and aggregations, while storage memory refers to that used for caching and propagating
 * internal data across the cluster. There exists one MemoryManager per JVM.
 */
private[spark] abstract class MemoryManager(
    conf: SparkConf,
    numCores: Int,
    onHeapStorageMemory: Long,
    onHeapExecutionMemory: Long) extends Logging {

  // -- Methods related to memory allocation policies and bookkeeping ------------------------------

  // 堆内内存缓存池
  @GuardedBy("this")
  protected val onHeapStorageMemoryPool = new StorageMemoryPool(this, MemoryMode.ON_HEAP)
  // 堆外内存缓存池
  @GuardedBy("this")
  protected val offHeapStorageMemoryPool = new StorageMemoryPool(this, MemoryMode.OFF_HEAP)
  // 堆内内存执行池
  @GuardedBy("this")
  protected val onHeapExecutionMemoryPool = new ExecutionMemoryPool(this, MemoryMode.ON_HEAP)
  // 堆外内存执行池
  @GuardedBy("this")
  protected val offHeapExecutionMemoryPool = new ExecutionMemoryPool(this, MemoryMode.OFF_HEAP)

  // 扩展堆内内存连接池
  onHeapStorageMemoryPool.incrementPoolSize(onHeapStorageMemory)
  // 拓展堆内内存执行池
  onHeapExecutionMemoryPool.incrementPoolSize(onHeapExecutionMemory)

  // 从配置文件中获取最大堆外可用内存
  protected[this] val maxOffHeapMemory = conf.get(MEMORY_OFFHEAP_SIZE)
  // 默认情况下 堆外内存和堆内内存比例是：1：1，各占50%
  protected[this] val offHeapStorageMemory =
    (maxOffHeapMemory * conf.getDouble("spark.memory.storageFraction", 0.5)).toLong

  // 拓展堆外内存执行池的内存，为堆外最大可用内存- 堆外内粗缓存池内存，这里很明显是50%
  offHeapExecutionMemoryPool.incrementPoolSize(maxOffHeapMemory - offHeapStorageMemory)
  // 拓展堆外内存缓存池的内存
  offHeapStorageMemoryPool.incrementPoolSize(offHeapStorageMemory)

  /**
   * 可用于存储的堆内存总量，以字节为单位。这个数量会随着时间的推移而变化，具体取决于 MemoryManager 的实现。
   * 在这个模型中，这相当于没有被执行占用的内存量。
   *
   * Total available on heap memory for storage, in bytes. This amount can vary over time,
   * depending on the MemoryManager implementation.
   * In this model, this is equivalent to the amount of memory not occupied by execution.
   */
  def maxOnHeapStorageMemory: Long

  /**
   * 用于存储的可用堆外内存总量，以字节为单位。这个数量会随着时间的推移而变化，具体取决于 MemoryManager 的实现。
   * Total available off heap memory for storage, in bytes. This amount can vary over time,
   * depending on the MemoryManager implementation.
   */
  def maxOffHeapStorageMemory: Long

  /**
   * 给onHeapStorageMemoryPool和offHeapStorageMemoryPool设置MemoryStore
   *
   * 设置此管理器使用的 MemoryStore 以驱逐缓存块。由于初始化顺序限制，这必须在构造后设置
   * Set the [[MemoryStore]] used by this manager to evict cached blocks.
   * This must be set after construction due to initialization ordering constraints.
   */
  final def setMemoryStore(store: MemoryStore): Unit = synchronized {
    onHeapStorageMemoryPool.setMemoryStore(store)
    offHeapStorageMemoryPool.setMemoryStore(store)
  }

  /**
   * 为存储BlockId对应的Block，从堆内存或堆外内存获取所需大小
   *
   * 获取 N 字节的内存来缓存给定的块，必要时驱逐现有的块。
   * Acquire N bytes of memory to cache the given block, evicting existing ones if necessary.
   *
   * @return whether all N bytes were successfully granted.
   */
  def acquireStorageMemory(blockId: BlockId, numBytes: Long, memoryMode: MemoryMode): Boolean

  /**
   * 为展开BlockId对应的Block，从堆内存或堆外内存获取所需大小（即numBytes）的内存
   *
   * 获取 N 字节的内存来展开给定的块，必要时驱逐现有的块。
     这个额外的方法允许子类区分获取存储内存和获取展开内存之间的行为。例如，Spark 1.5 及之前版本中的内存管理模型限制了可以从展开中释放的空间量。
     返回：
     是否所有 N 个字节都被成功授予。
   * Acquire N bytes of memory to unroll the given block, evicting existing ones if necessary.
   *
   * This extra method allows subclasses to differentiate behavior between acquiring storage
   * memory and acquiring unroll memory. For instance, the memory management model in Spark
   * 1.5 and before places a limit on the amount of space that can be freed from unrolling.
   *
   * @return whether all N bytes were successfully granted.
   */
  def acquireUnrollMemory(blockId: BlockId, numBytes: Long, memoryMode: MemoryMode): Boolean

  /**
   * 尝试为当前任务获取最多 numBytes 的执行内存并返回获取的字节数，如果无法分配则返回 0。
    在某些情况下，此调用可能会阻塞，直到有足够的可用内存，以确保每个任务在强制执行之前都有机会增加到总内存池的至少 1 / 2N（其中 N 是活动任务的数量）溢出。
    如果任务数量增加但较旧的任务已经有很多内存，就会发生这种情况。
   *
   * Try to acquire up to `numBytes` of execution memory for the current task and return the
   * number of bytes obtained, or 0 if none can be allocated.
   *
   * This call may block until there is enough free memory in some situations, to make sure each
   * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
   * active tasks) before it is forced to spill. This can happen if the number of tasks increase
   * but an older task had a lot of memory already.
   */
  private[memory]
  def acquireExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Long

  /**
   * 从堆内存或堆外内存释放指定大小（即numBytes）的内存
   *
   * 释放属于给定任务的 numBytes 执行内存。
   * Release numBytes of execution memory belonging to the given task.
   */
  private[memory]
  def releaseExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Unit = synchronized {
    memoryMode match {
      case MemoryMode.ON_HEAP => onHeapExecutionMemoryPool.releaseMemory(numBytes, taskAttemptId)
      case MemoryMode.OFF_HEAP => offHeapExecutionMemoryPool.releaseMemory(numBytes, taskAttemptId)
    }
  }

  /**
   * 释放执行内存
   * 释放给定任务的所有内存并将其标记为非活动（例如，当任务结束时）。
     返回：
     释放的字节数。
   *
   * Release all memory for the given task and mark it as inactive (e.g. when a task ends).
   *
   * @return the number of bytes freed.
   */
  private[memory] def releaseAllExecutionMemoryForTask(taskAttemptId: Long): Long = synchronized {
    // 释放堆内执行内存池的内存 + 释放堆外执行内存池的内存
    onHeapExecutionMemoryPool.releaseAllMemoryForTask(taskAttemptId) +
      offHeapExecutionMemoryPool.releaseAllMemoryForTask(taskAttemptId)
  }

  /**
   * 释放缓存内存
   *
   * Release N bytes of storage memory.
   */
  def releaseStorageMemory(numBytes: Long, memoryMode: MemoryMode): Unit = synchronized {
    memoryMode match {
      case MemoryMode.ON_HEAP => onHeapStorageMemoryPool.releaseMemory(numBytes)
      case MemoryMode.OFF_HEAP => offHeapStorageMemoryPool.releaseMemory(numBytes)
    }
  }

  /**
   * 释放所有的存储内存
   * Release all storage memory acquired.
   */
  final def releaseAllStorageMemory(): Unit = synchronized {
    onHeapStorageMemoryPool.releaseAllMemory()
    offHeapStorageMemoryPool.releaseAllMemory()
  }

  /**
   * 释放 N 字节的展开内存。
   * Release N bytes of unroll memory.
   */
  final def releaseUnrollMemory(numBytes: Long, memoryMode: MemoryMode): Unit = synchronized {
    releaseStorageMemory(numBytes, memoryMode)
  }

  /**
   * 当前使用的执行内存，以字节为单位。
   * Execution memory currently in use, in bytes.
   */
  final def executionMemoryUsed: Long = synchronized {
    onHeapExecutionMemoryPool.memoryUsed + offHeapExecutionMemoryPool.memoryUsed
  }

  /**
   * onHeapStorageMemoryPool与offHeapStorageMemoryPool中一共占用的存储内存
   *
   * 当前使用的存储内存，以字节为单位。
   * Storage memory currently in use, in bytes.
   */
  final def storageMemoryUsed: Long = synchronized {
    onHeapStorageMemoryPool.memoryUsed + offHeapStorageMemoryPool.memoryUsed
  }

  /**
   * 返回给定任务的执行内存消耗（以字节为单位）。
   * Returns the execution memory consumption, in bytes, for the given task.
   */
  private[memory] def getExecutionMemoryUsageForTask(taskAttemptId: Long): Long = synchronized {
    onHeapExecutionMemoryPool.getMemoryUsageForTask(taskAttemptId) +
      offHeapExecutionMemoryPool.getMemoryUsageForTask(taskAttemptId)
  }

  // -- Fields related to Tungsten managed memory -------------------------------------------------

  /**
   * Tracks whether Tungsten memory will be allocated on the JVM heap or off-heap using
   * sun.misc.Unsafe.
   */
  final val tungstenMemoryMode: MemoryMode = {
    if (conf.get(MEMORY_OFFHEAP_ENABLED)) {
      require(conf.get(MEMORY_OFFHEAP_SIZE) > 0,
        "spark.memory.offHeap.size must be > 0 when spark.memory.offHeap.enabled == true")
      require(Platform.unaligned(),
        "No support for unaligned Unsafe. Set spark.memory.offHeap.enabled to false.")
      MemoryMode.OFF_HEAP
    } else {
      MemoryMode.ON_HEAP
    }
  }

  /**
   * The default page size, in bytes.
   *
   * If user didn't explicitly set "spark.buffer.pageSize", we figure out the default value
   * by looking at the number of cores available to the process, and the total amount of memory,
   * and then divide it by a factor of safety.
   */
  val pageSizeBytes: Long = {
    val minPageSize = 1L * 1024 * 1024   // 1MB
    val maxPageSize = 64L * minPageSize  // 64MB
    val cores = if (numCores > 0) numCores else Runtime.getRuntime.availableProcessors()
    // Because of rounding to next power of 2, we may have safetyFactor as 8 in worst case
    val safetyFactor = 16
    val maxTungstenMemory: Long = tungstenMemoryMode match {
      case MemoryMode.ON_HEAP => onHeapExecutionMemoryPool.poolSize
      case MemoryMode.OFF_HEAP => offHeapExecutionMemoryPool.poolSize
    }
    val size = ByteArrayMethods.nextPowerOf2(maxTungstenMemory / cores / safetyFactor)
    val default = math.min(maxPageSize, math.max(minPageSize, size))
    conf.getSizeAsBytes("spark.buffer.pageSize", default)
  }

  /**
   * Allocates memory for use by Unsafe/Tungsten code.
   */
  private[memory] final val tungstenMemoryAllocator: MemoryAllocator = {
    tungstenMemoryMode match {
      case MemoryMode.ON_HEAP => MemoryAllocator.HEAP
      case MemoryMode.OFF_HEAP => MemoryAllocator.UNSAFE
    }
  }
}
