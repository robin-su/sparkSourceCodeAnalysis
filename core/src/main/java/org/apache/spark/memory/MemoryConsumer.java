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

package org.apache.spark.memory;

import java.io.IOException;

import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;

/**
 * 抽象类MemoryConsumer定义了内存消费者的规范，它通过TaskMemoryManager在执行内存（堆内存或堆外内存）上申请或释放内存。
 *
 * A memory consumer of {@link TaskMemoryManager} that supports spilling.
 *
 * Note: this only supports allocation / spilling of Tungsten memory.
 */
public abstract class MemoryConsumer {

  protected final TaskMemoryManager taskMemoryManager;
  // MemoryConsumer要消费的Page的大小。
  private final long pageSize;
  //即内存模式（MemoryMode）
  private final MemoryMode mode;
  // 当前消费者已经使用的执行内存的大小
  protected long used;

  protected MemoryConsumer(TaskMemoryManager taskMemoryManager, long pageSize, MemoryMode mode) {
    this.taskMemoryManager = taskMemoryManager;
    this.pageSize = pageSize;
    this.mode = mode;
  }

  protected MemoryConsumer(TaskMemoryManager taskMemoryManager) {
    this(taskMemoryManager, taskMemoryManager.pageSizeBytes(), MemoryMode.ON_HEAP);
  }

  /**
   * Returns the memory mode, {@link MemoryMode#ON_HEAP} or {@link MemoryMode#OFF_HEAP}.
   */
  public MemoryMode getMode() {
    return mode;
  }

  /**
   * Returns the size of used memory in bytes.
   */
  protected long getUsed() {
    return used;
  }

  /**
   * Force spill during building.
   */
  public void spill() throws IOException {
    spill(Long.MAX_VALUE, this);
  }

  /**
   * MemoryConsumer中定义了需要子类实现的抽象方法spill，当任务尝试没有足够的内存可用时，TaskMemoryManager将调用此方法把一些数据溢出到磁盘，以释放内存。
   *
   * Spill some data to disk to release memory, which will be called by TaskMemoryManager
   * when there is not enough memory for the task.
   *
   * This should be implemented by subclass.
   *
   * Note: In order to avoid possible deadlock, should not call acquireMemory() from spill().
   *
   * Note: today, this only frees Tungsten-managed pages.
   *
   * @param size the amount of memory should be released
   * @param trigger the MemoryConsumer that trigger this spilling
   * @return the amount of released memory in bytes
   * @throws IOException
   */
  public abstract long spill(long size, MemoryConsumer trigger) throws IOException;

  /**
   * 用于分配指定大小的长整型数组
   * Allocates a LongArray of `size`. Note that this method may throw `OutOfMemoryError` if Spark
   * doesn't have enough memory for this allocation, or throw `TooLargePageException` if this
   * `LongArray` is too large to fit in a single page. The caller side should take care of these
   * two exceptions, or make sure the `size` is small enough that won't trigger exceptions.
   *
   * @throws SparkOutOfMemoryError
   * @throws TooLargePageException
   */
  public LongArray allocateArray(long size) {
    //计算所需的Page大小（即required）。由于长整型占用8个字节，所以需要乘以8。
    long required = size * 8L; // 计算所需的page大小
    MemoryBlock page = taskMemoryManager.allocatePage(required, this);
    //如果分配得到的MemoryBlock的大小小于所需的大小required，则调用TaskMemory Manager的freePage方法。
    //
    if (page == null || page.size() < required) {
      throwOom(page, required);
    }
    used += required;
    return new LongArray(page);
  }

  /**
   * Frees a LongArray.
   */
  public void freeArray(LongArray array) {
    freePage(array.memoryBlock());
  }

  /**
   * Allocate a memory block with at least `required` bytes.
   *
   * @throws OutOfMemoryError
   */
  protected MemoryBlock allocatePage(long required) {
    MemoryBlock page = taskMemoryManager.allocatePage(Math.max(pageSize, required), this);
    if (page == null || page.size() < required) {
      throwOom(page, required);
    }
    used += page.size();
    return page;
  }

  /**
   * Free a memory block.
   */
  protected void freePage(MemoryBlock page) {
    used -= page.size();
    taskMemoryManager.freePage(page, this);
  }

  /**
   * Allocates memory of `size`.
   */
  public long acquireMemory(long size) {
    long granted = taskMemoryManager.acquireExecutionMemory(size, this);
    used += granted;
    return granted;
  }

  /**
   * Release N bytes of memory.
   */
  public void freeMemory(long size) {
    taskMemoryManager.releaseExecutionMemory(size, this);
    used -= size;
  }

  private void throwOom(final MemoryBlock page, final long required) {
    long got = 0;
    if (page != null) {
      got = page.size();
      taskMemoryManager.freePage(page, this);
    }
    taskMemoryManager.showMemoryUsage();
    throw new SparkOutOfMemoryError("Unable to acquire " + required + " bytes of memory, got " +
      got);
  }
}
