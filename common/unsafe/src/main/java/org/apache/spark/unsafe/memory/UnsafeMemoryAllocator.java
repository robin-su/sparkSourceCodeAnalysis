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

package org.apache.spark.unsafe.memory;

import org.apache.spark.unsafe.Platform;

/**
 * UnsafeMemoryAllocator是Tungsten在堆外内存模式下使用的内存分配器，与offHeap ExecutionMemoryPool配合使用
 *
 * A simple {@link MemoryAllocator} that uses {@code Unsafe} to allocate off-heap memory.
 */
public class UnsafeMemoryAllocator implements MemoryAllocator {

  /**
   * 用于分配指定大小（size）的MemoryBlock
   * @param size
   * @return
   * @throws OutOfMemoryError
   */
  @Override
  public MemoryBlock allocate(long size) throws OutOfMemoryError {
    // 在堆外内存分配指定大小的内存
    //在堆外内存分配指定大小的内存。Platform的allocateMemory方法实际代理了sun. misc.Unsafe的allocateMemory方法，
    // sun.misc.Unsafe的allocateMemory方法将返回分配的内存地址。
    long address = Platform.allocateMemory(size);
    // 创建MemoryBlock
    MemoryBlock memory = new MemoryBlock(null, address, size);
    if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
      memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
    }
    return memory;
  }

  /**
   * 用于释放MemoryBlock
   * ①：调用UnsafeMemoryAllocator的allocate方法分配MemoryBlock。
   * ②:UnsafeMemoryAllocator调用sun.misc.Unsafe的allocateMemory方法请求操作系统分配内存。
   * ③：操作系统分配了内存后，将此块内存的地址信息返回给UnsafeMemoryAllocator。UnsafeMemoryAllocator利用内存地址信息和内存大小创建MemoryBlock，
   * 此MemoryBlock的obj属性为null。
   * ④：调用UnsafeMemoryAllocator的free方法释放MemoryBlock。
   * ⑤：在调用UnsafeMemoryAllocator的free方法之前，调用方已经将此MemoryBlock的引用设置为null。
   * ⑥:UnsafeMemoryAllocator调用sun.misc.Unsafe的freeMemory方法请求操作系统释放内存。
   * ⑦:MemoryBlock的offset属性保存了Page在操作系统内存中的地址。
   * ⑧:MemoryBlock的length属性保存了Page的页
   *
   * @param memory
   */
  @Override
  public void free(MemoryBlock memory) {
    assert (memory.obj == null) :
      "baseObject not null; are you trying to use the off-heap allocator to free on-heap memory?";
    assert (memory.pageNumber != MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER) :
      "page has already been freed";
    assert ((memory.pageNumber == MemoryBlock.NO_PAGE_NUMBER)
            || (memory.pageNumber == MemoryBlock.FREED_IN_TMM_PAGE_NUMBER)) :
      "TMM-allocated pages must be freed via TMM.freePage(), not directly in allocator free()";

    if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
      memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_FREED_VALUE);
    }
    Platform.freeMemory(memory.offset);
    // As an additional layer of defense against use-after-free bugs, we mutate the
    // MemoryBlock to reset its pointer.
    memory.offset = 0;
    // Mark the page as freed (so we can detect double-frees).
    memory.pageNumber = MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER;
  }
}
