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

import java.util.concurrent.atomic.AtomicLong

import scala.reflect.ClassTag

import org.apache.commons.collections.map.{AbstractReferenceMap, ReferenceMap}

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.internal.Logging

/**
 * BroadcastManager用于将配置信息和序列化后的RDD,Job以及ShuffuleDependency等信息在本地存储。
 * 如果为了容灾，也会复制到其他j
 *
 * @param isDriver
 * @param conf
 * @param securityManager
 */
private[spark] class BroadcastManager(
    val isDriver: Boolean,
    conf: SparkConf,
    securityManager: SecurityManager)
  extends Logging {

  // 表示BraodcastManager是否初始化完成的状态
  private var initialized = false
  // 广播工厂实例
  private var broadcastFactory: BroadcastFactory = null

  initialize()

  /**
   * 在使用广播变量前会被SparkContext和Executor调用
   */
  // Called by SparkContext or Executor before using Broadcast
  private def initialize() {
    synchronized {
      // 保证BroadcastManager植被初始化一次
      if (!initialized) {
        // 实例化TorrentBroadcastFactory
        broadcastFactory = new TorrentBroadcastFactory
        // 初始化TorrentBroadcastFactory，其实什么都没干
        broadcastFactory.initialize(isDriver, conf, securityManager)
        initialized = true
      }
    }
  }

  def stop() {
    broadcastFactory.stop()
  }

  // 下一个广播对象的广播ID
  private val nextBroadcastId = new AtomicLong(0)

//  broadcastManager.cachedValues 保存着所有的 broadcast 的值，它是一个Map结构的，key是强引用，value是虚引用（在垃圾回收时会被清理掉）。
  private[broadcast] val cachedValues = {
    new ReferenceMap(AbstractReferenceMap.HARD, AbstractReferenceMap.WEAK)
  }

  /**
   * 实例化TorrentBroadcast类。
   *
   * @param value_
   * @param isLocal
   * @tparam T
   * @return
   */
  def newBroadcast[T: ClassTag](value_ : T, isLocal: Boolean): Broadcast[T] = {
    broadcastFactory.newBroadcast[T](value_, isLocal, nextBroadcastId.getAndIncrement())
  }

  /**
   * 调用TorrentBroadcast.unpersist方法使广播变量失效
   *
   * @param id
   * @param removeFromDriver
   * @param blocking
   */
  def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean) {
    broadcastFactory.unbroadcast(id, removeFromDriver, blocking)
  }
}
