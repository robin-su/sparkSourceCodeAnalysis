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

import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag

import com.google.common.collect.{ConcurrentHashMultiset, ImmutableMultiset}

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging


/**
 * 跟踪单个块的元数据。
 * Tracks metadata for an individual block.
 *
 * Instances of this class are _not_ thread-safe and are protected by locks in the
 * [[BlockInfoManager]].
 *
 * @param level the block's storage level. This is the requested persistence level, not the
 *              effective storage level of the block (i.e. if this is MEMORY_AND_DISK, then this
 *              does not imply that the block is actually resident in memory).
 * @param classTag the block's [[ClassTag]], used to select the serializer
 * @param tellMaster whether state changes for this block should be reported to the master. This
 *                   is true for most blocks, but is false for broadcast blocks.
 */
private[storage] class BlockInfo(
    val level: StorageLevel, // 存储级别
    val classTag: ClassTag[_], // block的对应类，用于选择序列化类
    val tellMaster: Boolean) { // block 的变化是否告知master。大部分情况下都是需要告知的，除了广播的block。

  /**
   * 块大小：单位字节
   * The size of the block (in bytes)
   */
  def size: Long = _size
  def size_=(s: Long): Unit = {
    _size = s
    checkInvariants()
  }
  private[this] var _size: Long = 0

  /**
   * 此块已被锁定以进行读取的次数。
   * The number of times that this block has been locked for reading.
   */
  def readerCount: Int = _readerCount
  def readerCount_=(c: Int): Unit = {
    _readerCount = c
    checkInvariants()
  }
  private[this] var _readerCount: Int = 0

  /**
   * 当前持有此块写锁的任务的任务尝试 id，或者 BlockInfo.NON_TASK_WRITER 如果写锁由非任务代码持有，或者 BlockInfo.NO_WRITER 如果此块未锁定写入
   * The task attempt id of the task which currently holds the write lock for this block, or
   * [[BlockInfo.NON_TASK_WRITER]] if the write lock is held by non-task code, or
   * [[BlockInfo.NO_WRITER]] if this block is not locked for writing.
   */
  def writerTask: Long = _writerTask
  def writerTask_=(t: Long): Unit = {
    _writerTask = t
    checkInvariants()
  }
  private[this] var _writerTask: Long = BlockInfo.NO_WRITER

  private def checkInvariants(): Unit = {
    // A block's reader count must be non-negative:
    assert(_readerCount >= 0)
    // A block is either locked for reading or for writing, but not for both at the same time:
    assert(_readerCount == 0 || _writerTask == BlockInfo.NO_WRITER)
  }

  checkInvariants()
}

private[storage] object BlockInfo {

  /**
   * 特殊标记：用于将块的写锁标记为"未锁定"的特殊常量
   * Special task attempt id constant used to mark a block's write lock as being unlocked.
   */
  val NO_WRITER: Long = -1

  /**
   * 特殊标记：特殊任务id，用于将块的写锁标记未"非任务"线程。
   * 也就是说：如果当前的写锁不是由任务线程持有，例如驱动程序线程或者单元测试线程，则将其标记未  NON_TASK_WRITER
   *
   * Special task attempt id constant used to mark a block's write lock as being held by
   * a non-task thread (e.g. by a driver thread or by unit test code).
   */
  val NON_TASK_WRITER: Long = -1024
}

/**
 * BlockManager 的组件，用于跟踪块的元数据并管理块锁定。
   这个类暴露的锁接口是读写锁。每个锁的获取都会自动与一个正在运行的任务相关联，并且在任务完成或失败时会自动释放锁。
   这个类是线程安全的。
 * Component of the [[BlockManager]] which tracks metadata for blocks and manages block locking.
 *
 * The locking interface exposed by this class is readers-writer lock. Every lock acquisition is
 * automatically associated with a running task and locks are automatically released upon task
 * completion or failure.
 *
 * This class is thread-safe.
 */
private[storage] class BlockInfoManager extends Logging {

  private type TaskAttemptId = Long

  /**
   * 保存了 Block-id 和 block信息的对应关系。
   *
   * 用于查找单个块的元数据。条目通过原子 set-if-not-exists 操作 (()) 添加到此映射中，并由 () 删除。
   * Used to look up metadata for individual blocks. Entries are added to this map via an atomic
   * set-if-not-exists operation ([[lockNewBlockForWriting()]]) and are removed
   * by [[removeBlock()]].
   */
  @GuardedBy("this")
  private[this] val infos = new mutable.HashMap[BlockId, BlockInfo]

  /**
   * 跟踪每个任务为写入而锁定的块集。
   * 每次任务执行尝试的标识TaskAttemptId与执行获取的Block的写锁之间的映射关系。
   * TaskAttemptId与写锁之间是一对多的关系，即一次任务尝试执行会获取零到多个Block的写锁
   *
   * Tracks the set of blocks that each task has locked for writing.
   */
  @GuardedBy("this")
  private[this] val writeLocksByTask =
    new mutable.HashMap[TaskAttemptId, mutable.Set[BlockId]]
      with mutable.MultiMap[TaskAttemptId, BlockId]

  /**
   * 每次任务尝试执行的标识TaskAttemptId与执行获取的Block的读锁之间的映射关系。
   * TaskAttemptId与读锁之间是一对多的关系，即一次任务尝试执行会获取零到多个Block的读锁，并且会记录对于同一个Block的读锁的占用次数。
   *
   * 跟踪每个任务为读取而锁定的块集，以及块被锁定的次数（因为我们的读取锁是可重入的）。
   * Tracks the set of blocks that each task has locked for reading, along with the number of times
   * that a block has been locked (since our read locks are re-entrant).
   */
  @GuardedBy("this")
  private[this] val readLocksByTask =
    new mutable.HashMap[TaskAttemptId, ConcurrentHashMultiset[BlockId]]

  // ----------------------------------------------------------------------------------------------

  // Initialization for special task attempt ids:
  registerTask(BlockInfo.NON_TASK_WRITER)

  // ----------------------------------------------------------------------------------------------

  /**
   * 在任务开始时调用，以便向此 BlockInfoManager 注册该任务。
   * 这必须在从该任务调用任何其他 BlockInfoManager 方法之前调用
   *
   * Called at the start of a task in order to register that task with this [[BlockInfoManager]].
   * This must be called prior to calling any other BlockInfoManager methods from that task.
   */
  def registerTask(taskAttemptId: TaskAttemptId): Unit = synchronized {
    // 若任务读锁中包含taskAttemptId，则说明已经被注册过
    require(!readLocksByTask.contains(taskAttemptId),
      s"Task attempt $taskAttemptId is already registered")
    readLocksByTask(taskAttemptId) = ConcurrentHashMultiset.create()
  }

  /**
   * 返回当前任务的任务尝试 ID（唯一标识任务），如果没有则返回非任务线程标识
   *
   * Returns the current task's task attempt id (which uniquely identifies the task), or
   * [[BlockInfo.NON_TASK_WRITER]] if called by a non-task thread.
   */
  private def currentTaskAttemptId: TaskAttemptId = {
    Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(BlockInfo.NON_TASK_WRITER)
  }

  /**
   * 在数据块上加上读锁，并返回其元数据，如果另一个任务已经锁定了这个块进行读取，那么读取锁将立即授予调用任务，并且它的锁计数将增加。
   * 如果另一个任务已锁定此块以进行写入，则此调用将阻塞，直到写入锁定被释放或如果阻塞 = false 将立即返回。
     单个任务可以多次锁定一个块进行读取，在这种情况下，每个锁都需要单独释放。
   * Lock a block for reading and return its metadata.
   *
   * If another task has already locked this block for reading, then the read lock will be
   * immediately granted to the calling task and its lock count will be incremented.
   *
   * If another task has locked this block for writing, then this call will block until the write
   * lock is released or will return immediately if `blocking = false`.
   *
   * A single task can lock a block multiple times for reading, in which case each lock will need
   * to be released separately.
   *
   * @param blockId the block to lock.
   * @param blocking if true (default), this call will block until the lock is acquired. If false,
   *                 this call will return immediately if the lock acquisition fails.
   *                 如果为 true（默认），则此调用将阻塞，直到获得锁。如果为 false，如果锁获取失败，此调用将立即返回。
   * @return None if the block did not exist or was removed (in which case no lock is held), or
   *         Some(BlockInfo) (in which case the block is locked for reading).
   */
  def lockForReading(
      blockId: BlockId,
      blocking: Boolean = true): Option[BlockInfo] = synchronized {
    logTrace(s"Task $currentTaskAttemptId trying to acquire read lock for $blockId")
    do {
      infos.get(blockId) match {
        case None => return None
        case Some(info) =>
          // 没有被写锁锁定
          if (info.writerTask == BlockInfo.NO_WRITER) {
            // 读取次数+1
            info.readerCount += 1
            // 往任务读锁列表添加当前块吗，用于记录任务的读锁
            readLocksByTask(currentTaskAttemptId).add(blockId)
            logTrace(s"Task $currentTaskAttemptId acquired read lock for $blockId")
            return Some(info)
          }
      }
      if (blocking) {
        wait() // 等待占用写锁的任务尝试线程释放Block的写锁后唤醒当前线程
      }
    } while (blocking)
    None
  }

  /**
   * 锁定块以进行写入并返回其元数据。
     如果另一个任务已经为读或写锁定了这个块，那么这个调用将阻塞直到其他锁被释放或者如果阻塞 = false 将立即返回。

   * Lock a block for writing and return its metadata.
   *
   * If another task has already locked this block for either reading or writing, then this call
   * will block until the other locks are released or will return immediately if `blocking = false`.
   *
   * @param blockId the block to lock.
   * @param blocking if true (default), this call will block until the lock is acquired. If false,
   *                 this call will return immediately if the lock acquisition fails.
   * @return None if the block did not exist or was removed (in which case no lock is held), or
   *         Some(BlockInfo) (in which case the block is locked for writing).
   */
  def lockForWriting(
      blockId: BlockId,
      blocking: Boolean = true): Option[BlockInfo] = synchronized {
    logTrace(s"Task $currentTaskAttemptId trying to acquire write lock for $blockId")
    do {
      infos.get(blockId) match {
        case None => return None
        case Some(info) =>
          // 块没有被写锁持有，且被读锁的次数为0，说明这个块也没有被任务在读取
          if (info.writerTask == BlockInfo.NO_WRITER && info.readerCount == 0) {
            // 将当前块的写入任务的ID设置为当前任务ID
            info.writerTask = currentTaskAttemptId
            // 写锁任务跟踪列表 + 1
            writeLocksByTask.addBinding(currentTaskAttemptId, blockId)
            logTrace(s"Task $currentTaskAttemptId acquired write lock for $blockId")
            return Some(info)
          }
      }

      /**
       * 如果允许阻塞（即blocking为true），那么当前线程将等待，直到占用写锁的线程释放Block的写锁后唤醒当前线程。
       * 如果占有写锁的线程一直不释放写锁，那么当前线程将出现“饥饿”状况，即可能无限期等待下去。
       */
      if (blocking) {
        wait()
      }
    } while (blocking)
    None
  }

  /**
   * 如果当前任务未在给定块上保持写锁，则引发异常。否则，返回块的BlockInfo。
   *
   * Throws an exception if the current task does not hold a write lock on the given block.
   * Otherwise, returns the block's BlockInfo.
   */
  def assertBlockIsLockedForWriting(blockId: BlockId): BlockInfo = synchronized {
    // 获取块信息
    infos.get(blockId) match {
      case Some(info) =>
        // 若果块的写锁持有者不是当前任务，抛出SparkException
        if (info.writerTask != currentTaskAttemptId) {
          throw new SparkException(
            s"Task $currentTaskAttemptId has not locked block $blockId for writing")
        } else {
          info
        }
      case None =>
        throw new SparkException(s"Block $blockId does not exist")
    }
  }

  /**
   * Get a block's metadata without acquiring any locks. This method is only exposed for use by
   * [[BlockManager.getStatus()]] and should not be called by other code outside of this class.
   */
  private[storage] def get(blockId: BlockId): Option[BlockInfo] = synchronized {
    infos.get(blockId)
  }

  /**
   * 将独占写锁降级为共享读锁。
   * Downgrades an exclusive write lock to a shared read lock.
   */
  def downgradeLock(blockId: BlockId): Unit = synchronized {
    logTrace(s"Task $currentTaskAttemptId downgrading write lock for $blockId")
    // 获取块信息
    val info = get(blockId).get
    // 降级必须降的是当前线程
    require(info.writerTask == currentTaskAttemptId,
      s"Task $currentTaskAttemptId tried to downgrade a write lock that it does not hold on" +
        s" block $blockId")
    unlock(blockId)
    val lockOutcome = lockForReading(blockId, blocking = false)
    assert(lockOutcome.isDefined)
  }

  /**
   * 释放给定块上的锁。如果 TaskContext 没有正确传播到任务的所有子线程，我们将无法从 TaskContext 获取 TID，因此我们必须显式传递 TID 值以释放锁。
   * Release a lock on the given block.
   * In case a TaskContext is not propagated properly to all child threads for the task, we fail to
   * get the TID from TaskContext, so we have to explicitly pass the TID value to release the lock.
   *
   * See SPARK-18406 for more discussion of this issue.
   */
  def unlock(blockId: BlockId, taskAttemptId: Option[TaskAttemptId] = None): Unit = synchronized {
    // 获取任务ID
    val taskId = taskAttemptId.getOrElse(currentTaskAttemptId)
    logTrace(s"Task $taskId releasing lock for $blockId")
    // 获取当前block信息
    val info = get(blockId).getOrElse {
      throw new IllegalStateException(s"Block $blockId not found")
    }
    // 若已经被"写锁"锁定
    if (info.writerTask != BlockInfo.NO_WRITER) {
      // 将其标记为"未锁定"
      info.writerTask = BlockInfo.NO_WRITER
      // 移除写锁
      writeLocksByTask.removeBinding(taskId, blockId)
    } else {
      assert(info.readerCount > 0, s"Block $blockId is not locked for reading")
      // 读的任务-1
      info.readerCount -= 1
      // 获取读取的块
      val countsForTask = readLocksByTask(taskId)
      // 移除读锁
      val newPinCountForTask: Int = countsForTask.remove(blockId, 1) - 1
      assert(newPinCountForTask >= 0,
        s"Task $taskId release lock on block $blockId more times than it acquired it")
    }
    notifyAll()
  }

  /**
   * Attempt to acquire the appropriate lock for writing a new block.
   *
   * This enforces the first-writer-wins semantics. If we are the first to write the block,
   * then just go ahead and acquire the write lock. Otherwise, if another thread is already
   * writing the block, then we wait for the write to finish before acquiring the read lock.
   *
   * @return true if the block did not already exist, false otherwise. If this returns false, then
   *         a read lock on the existing block will be held. If this returns true, a write lock on
   *         the new block will be held.
   */
  def lockNewBlockForWriting(
      blockId: BlockId,
      newBlockInfo: BlockInfo): Boolean = synchronized {
    logTrace(s"Task $currentTaskAttemptId trying to put $blockId")
    lockForReading(blockId) match {
      case Some(info) =>
        // Block already exists. This could happen if another thread races with us to compute
        // the same block. In this case, just keep the read lock and return.
        false
      case None =>
        // Block does not yet exist or is removed, so we are free to acquire the write lock
        infos(blockId) = newBlockInfo
        lockForWriting(blockId)
        true
    }
  }

  /**
   * Release all lock held by the given task, clearing that task's pin bookkeeping
   * structures and updating the global pin counts. This method should be called at the
   * end of a task (either by a task completion handler or in `TaskRunner.run()`).
   *
   * @return the ids of blocks whose pins were released
   */
  def releaseAllLocksForTask(taskAttemptId: TaskAttemptId): Seq[BlockId] = synchronized {
    val blocksWithReleasedLocks = mutable.ArrayBuffer[BlockId]()

    val readLocks = readLocksByTask.remove(taskAttemptId).getOrElse(ImmutableMultiset.of[BlockId]())
    val writeLocks = writeLocksByTask.remove(taskAttemptId).getOrElse(Seq.empty)

    for (blockId <- writeLocks) {
      infos.get(blockId).foreach { info =>
        assert(info.writerTask == taskAttemptId)
        info.writerTask = BlockInfo.NO_WRITER
      }
      blocksWithReleasedLocks += blockId
    }

    readLocks.entrySet().iterator().asScala.foreach { entry =>
      val blockId = entry.getElement
      val lockCount = entry.getCount
      blocksWithReleasedLocks += blockId
      get(blockId).foreach { info =>
        info.readerCount -= lockCount
        assert(info.readerCount >= 0)
      }
    }

    notifyAll()

    blocksWithReleasedLocks
  }

  /** Returns the number of locks held by the given task.  Used only for testing. */
  private[storage] def getTaskLockCount(taskAttemptId: TaskAttemptId): Int = {
    readLocksByTask.get(taskAttemptId).map(_.size()).getOrElse(0) +
      writeLocksByTask.get(taskAttemptId).map(_.size).getOrElse(0)
  }

  /**
   * Returns the number of blocks tracked.
   */
  def size: Int = synchronized {
    infos.size
  }

  /**
   * Return the number of map entries in this pin counter's internal data structures.
   * This is used in unit tests in order to detect memory leaks.
   */
  private[storage] def getNumberOfMapEntries: Long = synchronized {
    size +
      readLocksByTask.size +
      readLocksByTask.map(_._2.size()).sum +
      writeLocksByTask.size +
      writeLocksByTask.map(_._2.size).sum
  }

  /**
   * Returns an iterator over a snapshot of all blocks' metadata. Note that the individual entries
   * in this iterator are mutable and thus may reflect blocks that are deleted while the iterator
   * is being traversed.
   */
  def entries: Iterator[(BlockId, BlockInfo)] = synchronized {
    infos.toArray.toIterator
  }

  /**
   * Removes the given block and releases the write lock on it.
   *
   * This can only be called while holding a write lock on the given block.
   */
  def removeBlock(blockId: BlockId): Unit = synchronized {
    logTrace(s"Task $currentTaskAttemptId trying to remove block $blockId")
    infos.get(blockId) match {
      case Some(blockInfo) =>
        if (blockInfo.writerTask != currentTaskAttemptId) {
          throw new IllegalStateException(
            s"Task $currentTaskAttemptId called remove() on block $blockId without a write lock")
        } else {
          infos.remove(blockId)
          blockInfo.readerCount = 0
          blockInfo.writerTask = BlockInfo.NO_WRITER
          writeLocksByTask.removeBinding(currentTaskAttemptId, blockId)
        }
      case None =>
        throw new IllegalArgumentException(
          s"Task $currentTaskAttemptId called remove() on non-existent block $blockId")
    }
    notifyAll()
  }

  /**
   * Delete all state. Called during shutdown.
   */
  def clear(): Unit = synchronized {
    infos.valuesIterator.foreach { blockInfo =>
      blockInfo.readerCount = 0
      blockInfo.writerTask = BlockInfo.NO_WRITER
    }
    infos.clear()
    readLocksByTask.clear()
    writeLocksByTask.clear()
    notifyAll()
  }

}
