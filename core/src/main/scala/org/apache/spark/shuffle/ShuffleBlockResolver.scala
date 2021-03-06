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

import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.storage.ShuffleBlockId

private[spark]
/**
 *  主要用于shuffle blocks从逻辑block到物理文件之间的映射关系。实现了获取Shuffle数据文件、获取Shuffle索引文件、
 *  删除指定的Shuffle数据文件和索引文件、生成Shuffle索引文件、获取Shuffle块的数据等
 *
 * Implementers of this trait understand how to retrieve block data for a logical shuffle block
 * identifier (i.e. map, reduce, and shuffle). Implementations may use files or file segments to
 * encapsulate shuffle data. This is used by the BlockStore to abstract over different shuffle
 * implementations when shuffle data is retrieved.
 */
trait ShuffleBlockResolver {
  type ShuffleId = Int

  /**
   * 检索指定块的数据。如果该块的数据不可用，则抛出未指定的异常。
   *
   * Retrieve the data for the specified block. If the data for that block is not available,
   * throws an unspecified exception.
   */
  def getBlockData(blockId: ShuffleBlockId): ManagedBuffer

  def stop(): Unit
}
