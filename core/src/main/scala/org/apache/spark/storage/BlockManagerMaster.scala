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

package org.apache.spark.storage

import scala.collection.Iterable
import scala.collection.generic.CanBuildFrom
import scala.concurrent.Future

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.util.{RpcUtils, ThreadUtils}

/**
 * BlockManagerMaster 主要负责和driver的交互，来获取跟底层存储相关的信息。
 * 代理BlockManager与Driver上的BlockManagerMasterEndpoint通信
 *
 * @param driverEndpoint
 * @param conf
 * @param isDriver
 */
private[spark]
class BlockManagerMaster(
    var driverEndpoint: RpcEndpointRef,
    conf: SparkConf,
    isDriver: Boolean)
  extends Logging {

  // timeout 是指的 Spark RPC 超时时间，默认为 120s，可以通过spark.rpc.askTimeout 或 spark.network.timeout 参数来设置。
  val timeout = RpcUtils.askRpcTimeout(conf)

  /**
   * 移除"失效"的executor,该方法只能在Driver端进行调用
   * Remove a dead executor from the driver endpoint. This is only called on the driver side.
   */
  def removeExecutor(execId: String) {
    tell(RemoveExecutor(execId))
    logInfo("Removed " + execId + " successfully in removeExecutor")
  }

  /**
   *  异步的移除"失效"的executor,该方法只能在Driver段调用
   *  Request removal of a dead executor from the driver endpoint.
   *  This is only called on the driver side. Non-blocking
   */
  def removeExecutorAsync(execId: String) {
    driverEndpoint.ask[Boolean](RemoveExecutor(execId))
    logInfo("Removal of executor " + execId + " requested")
  }

  /**
   * 向driver注册 BlockManager 的 id。输入的 BlockManagerId 不包含拓扑信息。
   * 此信息是从master获取的，我们以更新的 BlockManagerId 响应，并用此信息充实。
   *
   * Register the BlockManager's id with the driver. The input BlockManagerId does not contain
   * topology information. This information is obtained from the master and we respond with an
   * updated BlockManagerId fleshed out with this information.
   */
  def registerBlockManager(
      blockManagerId: BlockManagerId,
      maxOnHeapMemSize: Long,
      maxOffHeapMemSize: Long,
      slaveEndpoint: RpcEndpointRef): BlockManagerId = {
    logInfo(s"Registering BlockManager $blockManagerId")
    // 向driver注册blockManager,本质上是通过netty rpc远程调用
    val updatedId = driverEndpoint.askSync[BlockManagerId](
      RegisterBlockManager(blockManagerId, maxOnHeapMemSize, maxOffHeapMemSize, slaveEndpoint))
    logInfo(s"Registered BlockManager $updatedId")
    updatedId
  }

  /**
   * 更新block信息
   *
   * @param blockManagerId
   * @param blockId
   * @param storageLevel
   * @param memSize
   * @param diskSize
   * @return
   */
  def updateBlockInfo(
      blockManagerId: BlockManagerId,
      blockId: BlockId,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long): Boolean = {
    val res = driverEndpoint.askSync[Boolean](
      UpdateBlockInfo(blockManagerId, blockId, storageLevel, memSize, diskSize))
    logDebug(s"Updated info of block $blockId")
    res
  }

  /**
   * 从driver端获取blockId的位置信息
   *
   * Get locations of the blockId from the driver
   */
  def getLocations(blockId: BlockId): Seq[BlockManagerId] = {
    driverEndpoint.askSync[Seq[BlockManagerId]](GetLocations(blockId))
  }

  /**
   * 从driver端，获取位置信息和状态
   * Get locations as well as status of the blockId from the driver
   */
  def getLocationsAndStatus(blockId: BlockId): Option[BlockLocationsAndStatus] = {
    driverEndpoint.askSync[Option[BlockLocationsAndStatus]](
      GetLocationsAndStatus(blockId))
  }

  /**
   * 从driver获取多个 blockId 的位置
   *
   * Get locations of multiple blockIds from the driver
   */
  def getLocations(blockIds: Array[BlockId]): IndexedSeq[Seq[BlockManagerId]] = {
    driverEndpoint.askSync[IndexedSeq[Seq[BlockManagerId]]](
      GetLocationsMultipleBlockIds(blockIds))
  }

  /**
   * 检查块管理器 master 是否有块。请注意，这可用于仅检查报告给块管理器主机的那些块。
   *
   * Check if block manager master has a block. Note that this can be used to check for only
   * those blocks that are reported to block manager master.
   */
  def contains(blockId: BlockId): Boolean = {
    !getLocations(blockId).isEmpty
  }

  /**
   * 向driver 请求获得集群中所有的 blockManager的信息
   *
   * Get ids of other nodes in the cluster from the driver
   */
  def getPeers(blockManagerId: BlockManagerId): Seq[BlockManagerId] = {
    driverEndpoint.askSync[Seq[BlockManagerId]](GetPeers(blockManagerId))
  }

  /**
   * 向driver 请求executor endpoint ref 对象

   * @param executorId
   * @return
   */
  def getExecutorEndpointRef(executorId: String): Option[RpcEndpointRef] = {
    driverEndpoint.askSync[Option[RpcEndpointRef]](GetExecutorEndpointRef(executorId))
  }

  /**
   * 从拥有它的从站中删除一个块。这只能用于删除驱动程序知道的块。
   *
   * Remove a block from the slaves that have it. This can only be used to remove
   * blocks that the driver knows about.
   */
  def removeBlock(blockId: BlockId) {
    driverEndpoint.askSync[Boolean](RemoveBlock(blockId))
  }

  /**
   * 删除属于给定 RDD 的所有块。
   *
   * Remove all blocks belonging to the given RDD.
   */
  def removeRdd(rddId: Int, blocking: Boolean) {
    val future = driverEndpoint.askSync[Future[Seq[Int]]](RemoveRdd(rddId))
    future.failed.foreach(e =>
      logWarning(s"Failed to remove RDD $rddId - ${e.getMessage}", e)
    )(ThreadUtils.sameThread)
    if (blocking) {
      timeout.awaitResult(future)
    }
  }

  /**
   * 删除属于给定 shuffle 的所有块。
   *
   * Remove all blocks belonging to the given shuffle.
   */
  def removeShuffle(shuffleId: Int, blocking: Boolean) {
    val future = driverEndpoint.askSync[Future[Seq[Boolean]]](RemoveShuffle(shuffleId))
    future.failed.foreach(e =>
      logWarning(s"Failed to remove shuffle $shuffleId - ${e.getMessage}", e)
    )(ThreadUtils.sameThread)
    if (blocking) {
      timeout.awaitResult(future)
    }
  }

  /**
   * 删除属于给定广播的所有块。
   *
   * Remove all blocks belonging to the given broadcast.
   */
  def removeBroadcast(broadcastId: Long, removeFromMaster: Boolean, blocking: Boolean) {
    val future = driverEndpoint.askSync[Future[Seq[Int]]](
      RemoveBroadcast(broadcastId, removeFromMaster))
    future.failed.foreach(e =>
      logWarning(s"Failed to remove broadcast $broadcastId" +
        s" with removeFromMaster = $removeFromMaster - ${e.getMessage}", e)
    )(ThreadUtils.sameThread)
    if (blocking) {
      timeout.awaitResult(future)
    }
  }

  /**
   * 向driver 请求获取每一个BlockManager内存状态。
   * 第一个值是为块管理器分配的最大内存量，而第二个值是剩余内存量。
   *
   * Return the memory status for each block manager, in the form of a map from
   * the block manager's id to two long values. The first value is the maximum
   * amount of memory allocated for the block manager, while the second is the
   * amount of remaining memory.
   */
  def getMemoryStatus: Map[BlockManagerId, (Long, Long)] = {
    if (driverEndpoint == null) return Map.empty
    driverEndpoint.askSync[Map[BlockManagerId, (Long, Long)]](GetMemoryStatus)
  }

  /**
   * 向driver请求获取磁盘状态
   * @return
   */
  def getStorageStatus: Array[StorageStatus] = {
    if (driverEndpoint == null) return Array.empty
    driverEndpoint.askSync[Array[StorageStatus]](GetStorageStatus)
  }

  /**
   * 向driver请求获取block状态
   *
   * Return the block's status on all block managers, if any. NOTE: This is a
   * potentially expensive operation and should only be used for testing.
   *
   * If askSlaves is true, this invokes the master to query each block manager for the most
   * updated block statuses. This is useful when the master is not informed of the given block
   * by all block managers.
   */
  def getBlockStatus(
      blockId: BlockId,
      askSlaves: Boolean = true): Map[BlockManagerId, BlockStatus] = {
    val msg = GetBlockStatus(blockId, askSlaves)
    /*
     * To avoid potential deadlocks, the use of Futures is necessary, because the master endpoint
     * should not block on waiting for a block manager, which can in turn be waiting for the
     * master endpoint for a response to a prior message.
     */
    val response = driverEndpoint.
      askSync[Map[BlockManagerId, Future[Option[BlockStatus]]]](msg)
    val (blockManagerIds, futures) = response.unzip
    implicit val sameThread = ThreadUtils.sameThread
    val cbf =
      implicitly[
        CanBuildFrom[Iterable[Future[Option[BlockStatus]]],
        Option[BlockStatus],
        Iterable[Option[BlockStatus]]]]
    val blockStatus = timeout.awaitResult(
      Future.sequence[Option[BlockStatus], Iterable](futures)(cbf, ThreadUtils.sameThread))
    if (blockStatus == null) {
      throw new SparkException("BlockManager returned null for BlockStatus query: " + blockId)
    }
    blockManagerIds.zip(blockStatus).flatMap { case (blockManagerId, status) =>
      status.map { s => (blockManagerId, s) }
    }.toMap
  }

  /**
   * 是否有匹配的block
   *
   * Return a list of ids of existing blocks such that the ids match the given filter. NOTE: This
   * is a potentially expensive operation and should only be used for testing.
   *
   * If askSlaves is true, this invokes the master to query each block manager for the most
   * updated block statuses. This is useful when the master is not informed of the given block
   * by all block managers.
   */
  def getMatchingBlockIds(
      filter: BlockId => Boolean,
      askSlaves: Boolean): Seq[BlockId] = {
    val msg = GetMatchingBlockIds(filter, askSlaves)
    val future = driverEndpoint.askSync[Future[Seq[BlockId]]](msg)
    timeout.awaitResult(future)
  }

  /**
   * 查明executor是否已缓存块。这种方法不考虑广播块，因为它们没有被报告为主。
   *
   * Find out if the executor has cached blocks. This method does not consider broadcast blocks,
   * since they are not reported the master.
   */
  def hasCachedBlocks(executorId: String): Boolean = {
    driverEndpoint.askSync[Boolean](HasCachedBlocks(executorId))
  }

  /**
   * 停止driver endpoint,只能被spark的driver节点调用
   * Stop the driver endpoint, called only on the Spark driver node
   */
  def stop() {
    if (driverEndpoint != null && isDriver) {
      tell(StopBlockManagerMaster)
      driverEndpoint = null
      logInfo("BlockManagerMaster stopped")
    }
  }

  /**
   * 向主端点发送单向消息，我们希望它以 true 回复。
   *
   * Send a one-way message to the master endpoint, to which we expect it to reply with true.
   */
  private def tell(message: Any) {
    if (!driverEndpoint.askSync[Boolean](message)) {
      throw new SparkException("BlockManagerMasterEndpoint returned false, expected true.")
    }
  }

}

private[spark] object BlockManagerMaster {
  val DRIVER_ENDPOINT_NAME = "BlockManagerMaster"
}
