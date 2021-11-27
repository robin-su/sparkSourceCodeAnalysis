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

package org.apache.spark.util.collection

import scala.collection.mutable

import org.apache.spark.util.SizeEstimator

/**
 * SizeTracker定义了对集合进行采样和估算的规范。
 *
 * A general interface for collections to keep track of their estimated sizes in bytes.
 * We sample with a slow exponential back-off using the SizeEstimator to amortize the time,
 * as each call to SizeEstimator is somewhat expensive (order of a few milliseconds).
 */
private[spark] trait SizeTracker {

  import SizeTracker._

  /**
   * 采样增长的速率，类似于等差数列的等差
   *
   * Controls the base of the exponential which governs the rate of sampling.
   * E.g., a value of 2 would mean we sample at 1, 2, 4, 8, ... elements.
   */
  private val SAMPLE_GROWTH_RATE = 1.1

  /**
   * 样本队列。最后两个样本将被用于估算。
   *
   * Samples taken since last resetSamples(). Only the last two are kept for extrapolation.
   */
  private val samples = new mutable.Queue[Sample]

  /**
   * 平均每次更新的字节数。
   *
   * The average number of bytes per update between our last two samples.
   */
  private var bytesPerUpdate: Double = _

  /**
   * 更新操作（包括插入和更新）的总次数。
   *
   * Total number of insertions and updates into the map since the last resetSamples().
   */
  private var numUpdates: Long = _

  /**
   * 下次采样时，numUpdates的值，即numUpdates的值增长到与nextSampleNum相同时，才会再次采样。
   *
   *  The value of 'numUpdates' at which we will take our next sample.
   */
  private var nextSampleNum: Long = _

  resetSamples()

  /**
   * 重置SizeTracker采集的样本
   *
   * Reset samples collected so far.
   * This should be called after the collection undergoes a dramatic change in size.
   */
  protected def resetSamples(): Unit = {
    numUpdates = 1
    nextSampleNum = 1
    samples.clear()
    takeSample()
  }

  /**
   * 用于向集合中更新了元素之后进行回调，以触发对集合的采样
   *
   * Callback to be invoked after every update.
   */
  protected def afterUpdate(): Unit = {
    numUpdates += 1
    if (nextSampleNum == numUpdates) {
      takeSample()
    }
  }

  /**
   * Take a new sample of the current collection's size.
   */
  private def takeSample(): Unit = {
    // 估算集合的大小作为样本
    samples.enqueue(Sample(SizeEstimator.estimate(this), numUpdates))
    // Only use the last two samples to extrapolate
    // 保留样本队列最后两个样本
    if (samples.size > 2) {
      samples.dequeue()
    }
    val bytesDelta = samples.toList.reverse match {
      case latest :: previous :: tail =>
        (latest.size - previous.size).toDouble / (latest.numUpdates - previous.numUpdates)
      // If fewer than 2 samples, assume no change
      case _ => 0
    }
    // 计算每次更新的字节数
    bytesPerUpdate = math.max(0, bytesDelta)
    // 机选下次采样的采样号
    nextSampleNum = math.ceil(numUpdates * SAMPLE_GROWTH_RATE).toLong
  }

  /**
   * 用于估算集合的当前大小（单位为字节）
   *
   * Estimate the current size of the collection in bytes. O(1) time.
   */
  def estimateSize(): Long = {
    assert(samples.nonEmpty)
    val extrapolatedDelta = bytesPerUpdate * (numUpdates - samples.last.numUpdates)
    (samples.last.size + extrapolatedDelta).toLong
  }
}

private object SizeTracker {
  case class Sample(size: Long, numUpdates: Long)
}
