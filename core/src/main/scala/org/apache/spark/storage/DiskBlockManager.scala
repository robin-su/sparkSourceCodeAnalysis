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

import java.io.{File, IOException}
import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.internal.Logging
import org.apache.spark.util.{ShutdownHookManager, Utils}

/**
 * 可以看出，这个类主要用于创建并维护逻辑block和block落地文件的映射关系。保存映射关系，有两个解决方案：
 *  一者是使用Map存储每一条具体的映射键值对，
 *  二者是指定映射函数像分区函数等等，给定的key通过映射函数映射到具体的value。
 *  deleteFilesOnStop：停止DiskBlockManager的时候是否删除本地目录的布尔类型标记。
 *  当不指定外部的ShuffleClient（即spark.shuffle.service.enabled属性为false）或者当前实例是Driver时，此属性为true
 *
 * Creates and maintains the logical mapping between logical blocks and physical on-disk
 * locations. One block is mapped to one file with a name given by its BlockId.
 *
 * Block files are hashed among the directories listed in spark.local.dir (or in
 * SPARK_LOCAL_DIRS, if it's set).
 */
private[spark] class DiskBlockManager(conf: SparkConf, deleteFilesOnStop: Boolean) extends Logging {

  // 这个变量表示本地文件下有几个文件，默认为64
  private[spark] val subDirsPerLocalDir = conf.getInt("spark.diskStore.subDirectories", 64)

  /**
   * 为 spark.local.dir 中提到的每个路径创建一个本地目录；然后，在这个目录中，创建多个子目录，我们将把文件散列到这些子目录中，以避免在顶层有非常大的 inode。
   *  Create one local directory for each path mentioned in spark.local.dir; then, inside this
   * directory, create multiple subdirectories that we will hash files into, in order to avoid
   * having really large inodes at the top level. */
  // 表示block落地本地文件根目录,创建用于存储block信息的本地文件
  private[spark] val localDirs: Array[File] = createLocalDirs(conf)
  if (localDirs.isEmpty) {
    logError("Failed to create any local dir.")
    System.exit(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR)
  }
  // The content of subDirs is immutable but the content of subDirs(i) is mutable. And the content
  // of subDirs(i) is protected by the lock of subDirs(i)
  private val subDirs = Array.fill(localDirs.length)(new Array[File](subDirsPerLocalDir))

  private val shutdownHook = addShutdownHook()

  /**
   * 根据指定的文件名获取文件
   * Looks up a file by hashing it into one of our local subdirectories. */
  // This method should be kept in sync with
  // org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getFile().
  def getFile(filename: String): File = {
    // Figure out which local directory it hashes to, and which subdirectory in that
    // 调用Utils工具类的nonNegativeHash方法获取文件名的非负哈希值
    val hash = Utils.nonNegativeHash(filename)
    // 从localDirs数组中按照取余方式获得选中的一级目录
    val dirId = hash % localDirs.length

    // 哈希值以一级目录的大小获得商，然后用商数与subDirsPerLocalDir取作获得的余数作为选中的二级目录
    val subDirId = (hash / localDirs.length) % subDirsPerLocalDir

    // Create the subdirectory if it doesn't already exist
    // 获取二级目录。如果二级目录不存在，则需要创建二级目录
    val subDir = subDirs(dirId).synchronized {
      val old = subDirs(dirId)(subDirId)
      if (old != null) {
        old
      } else {
        val newDir = new File(localDirs(dirId), "%02x".format(subDirId))
        if (!newDir.exists() && !newDir.mkdir()) {
          throw new IOException(s"Failed to create local dir in $newDir.")
        }
        subDirs(dirId)(subDirId) = newDir
        newDir
      }
    }

    new File(subDir, filename)
  }

  /**
   * 此方法根据BlockId获取文件。
   *
   * @param blockId
   * @return
   */
  def getFile(blockId: BlockId): File = getFile(blockId.name)

  /** Check if disk block manager has a block. */
  def containsBlock(blockId: BlockId): Boolean = {
    getFile(blockId.name).exists()
  }

  /**
   * 此方法用于获取本地localDirs目录中的所有文件
   *
   * getAllFiles为了保证线程安全，采用了同步+克隆的方式。
   *
   * dirId 是指的第几个父目录（从0开始数），subDirId是指的父目录下的第几个子目录（从0开始数）。最后拼接父子目录为一个新的父目录subDir。
   然后以subDir为父目录，创建File对象，并返回之。
   * List all the files currently stored on disk by the disk manager.
   */
  def getAllFiles(): Seq[File] = {
    // Get all the files inside the array of array of directories
    subDirs.flatMap { dir =>
      dir.synchronized {
        // Copy the content of dir because it may be modified in other threads
        dir.clone()
      }
    }.filter(_ != null).flatMap { dir =>
      val files = dir.listFiles()
      if (files != null) files else Seq.empty
    }
  }

  /**
   *  此方法用于获取本地localDirs目录中的所有文件
   *
   *  List all the blocks currently stored on disk by the disk manager.
   */
  def getAllBlocks(): Seq[BlockId] = {
    getAllFiles().flatMap { f =>
      try {
        Some(BlockId(f.getName))
      } catch {
        case _: UnrecognizedBlockId =>
          // Skip files which do not correspond to blocks, for example temporary
          // files created by [[SortShuffleWriter]].
          None
      }
    }
  }

  /**
   * 此方法用于为'中间结果'创建唯一的BlockId和文件，此文件将用于保存本地Block的数据。
   *
   * 此方法用于获取本地localDirs目录中的所有文件
   * Produces a unique block id and File suitable for storing local intermediate results.
   */
  def createTempLocalBlock(): (TempLocalBlockId, File) = {
    var blockId = new TempLocalBlockId(UUID.randomUUID())
    while (getFile(blockId).exists()) {
      blockId = new TempLocalBlockId(UUID.randomUUID())
    }
    (blockId, getFile(blockId))
  }

  /**
   * 此方法创建唯一的BlockId和文件，用来存储Shuffle中间结果（即map任务的输出）

   * Produces a unique block id and File suitable for storing shuffled intermediate results.
   */
  def createTempShuffleBlock(): (TempShuffleBlockId, File) = {
    var blockId = new TempShuffleBlockId(UUID.randomUUID())
    while (getFile(blockId).exists()) {
      blockId = new TempShuffleBlockId(UUID.randomUUID())
    }
    (blockId, getFile(blockId))
  }

  /**
   * 创建用于存储块数据的本地目录。这些目录位于配置的本地目录中，在使用外部 shuffle 服务时不会在 JVM 退出时被删除。
   *
   * getConfiguredLocalDirs方法默认获取spark.local.dir属性或者系统属性java.io.tmpdir指定的目录，目录可能有多个），
   * 并在每个路径下创建以blockmgr-为前缀，UUID为后缀的随机字符串的子目录，例如，blockmgr-4949e19c-490c-48fc-ad6a-d80f4dbe73df。
   *
   * Create local directories for storing block data. These directories are
   * located inside configured local directories and won't
   * be deleted on JVM exit when using the external shuffle service.
   */
  private def createLocalDirs(conf: SparkConf): Array[File] = {
    Utils.getConfiguredLocalDirs(conf).flatMap { rootDir =>
      try {
        val localDir = Utils.createDirectory(rootDir, "blockmgr")
        logInfo(s"Created local directory at $localDir")
        Some(localDir)
      } catch {
        case e: IOException =>
          logError(s"Failed to create local dir in $rootDir. Ignoring this directory.", e)
          None
      }
    }
  }

  private def addShutdownHook(): AnyRef = {
    logDebug("Adding shutdown hook") // force eager creation of logger
    ShutdownHookManager.addShutdownHook(ShutdownHookManager.TEMP_DIR_SHUTDOWN_PRIORITY + 1) { () =>
      logInfo("Shutdown hook called")
      DiskBlockManager.this.doStop()
    }
  }

  /**
   * 此方法用于正常停止DiskBlockManager
   * Cleanup local dirs and stop shuffle sender.
   */
  private[spark] def stop() {
    // Remove the shutdown hook.  It causes memory leaks if we leave it around.
    try {
      ShutdownHookManager.removeShutdownHook(shutdownHook)
    } catch {
      case e: Exception =>
        logError(s"Exception while removing shutdown hook.", e)
    }
    doStop()
  }

  /**
   * 实际停止DiskBlockManager的方法为doStop
   *
   * doStop的主要逻辑是遍历localDirs数组中的一级目录，并调用工具类Utils的deleteRecuresively方法，递归删除一级目录及其子目录或子文件。
   */
  private def doStop(): Unit = {
    if (deleteFilesOnStop) {
      localDirs.foreach { localDir =>
        if (localDir.isDirectory() && localDir.exists()) {
          try {
            if (!ShutdownHookManager.hasRootAsShutdownDeleteDir(localDir)) {
              Utils.deleteRecursively(localDir)
            }
          } catch {
            case e: Exception =>
              logError(s"Exception while deleting local spark dir: $localDir", e)
          }
        }
      }
    }
  }
}
