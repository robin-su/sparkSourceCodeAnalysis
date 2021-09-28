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

package org.apache.spark.scheduler

import scala.collection.mutable

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.util.{RpcUtils, ThreadUtils}

private sealed trait OutputCommitCoordinationMessage extends Serializable

private case object StopCoordinator extends OutputCommitCoordinationMessage
private case class AskPermissionToCommitOutput(
    stage: Int,
    stageAttempt: Int,
    partition: Int,
    attemptNumber: Int)

/**
 * 用于判定给定Stage的分区任务是否有权限将输出提交到HDFS，并对同一分区任务的多次任务尝试（TaskAttempt）进行协调
 *
 * 当Spark应用程序使用了Spark SQL（包括Hive）或者需要将任务的输出保存到HDFS时，就会用到输出提交协调器OutputCommitCoordinator,
 * OutputCommitCoordinator将决定任务是否可以提交输出到HDFS。无论是Driver还是Executor，在SparkEnv中都包含了子组件OutputCommitCoordinator。
 * 在Driver上注册了OutputCommitCoordinatorEndpoint，所有Executor上的OutputCommitCoordinator都是通过OutputCommitCoordinatorEndpoint
 * 的RpcEndpointRef来询问Driver上的OutputCommitCoordinator，是否能够将输出提交到HDFS。
 *
 * Authority that decides whether tasks can commit output to HDFS. Uses a "first committer wins"
 * policy.
 *
 * OutputCommitCoordinator is instantiated in both the drivers and executors. On executors, it is
 * configured with a reference to the driver's OutputCommitCoordinatorEndpoint, so requests to
 * commit output will be forwarded to the driver's OutputCommitCoordinator.
 *
 * This class was introduced in SPARK-4879; see that JIRA issue (and the associated pull requests)
 * for an extensive design discussion.
 */
private[spark] class OutputCommitCoordinator(conf: SparkConf, isDriver: Boolean) extends Logging {

  // Initialized by SparkEnv
  var coordinatorRef: Option[RpcEndpointRef] = None

  // Class used to identify a committer. The task ID for a committer is implicitly defined by
  // the partition being processed, but the coordinator needs to keep track of both the stage
  // attempt and the task attempt, because in some situations the same task may be running
  // concurrently in two different attempts of the same stage.
  private case class TaskIdentifier(stageAttempt: Int, taskAttempt: Int)

  private case class StageState(numPartitions: Int) {
    val authorizedCommitters = Array.fill[TaskIdentifier](numPartitions)(null)
    val failures = mutable.Map[Int, mutable.Set[TaskIdentifier]]()
  }

  /**
   * 用于判断authorizedCommittersByStage是否为
   *
   * Map from active stages's id => authorized task attempts for each partition id, which hold an
   * exclusive lock on committing task output for that partition, as well as any known failed
   * attempts in the stage.
   *
   * Entries are added to the top-level map when stages start and are removed they finish
   * (either successfully or unsuccessfully).
   *
   * Access to this map should be guarded by synchronizing on the OutputCommitCoordinator instance.
   */
  private val stageStates = mutable.Map[Int, StageState]()

  /**
   * Returns whether the OutputCommitCoordinator's internal data structures are all empty.
   */
  def isEmpty: Boolean = {
    stageStates.isEmpty
  }

  /**
   * 用于向OutputCommitCoordinatorEndpoint发送AskPermissionToCommitOutput，
   * 并根据OutputCommitCoordinatorEndpoint的响应确认是否有权限将Stage的指定分区的输出提交到HDFS上
   *
   * Called by tasks to ask whether they can commit their output to HDFS.
   *
   * If a task attempt has been authorized to commit, then all other attempts to commit the same
   * task will be denied.  If the authorized task attempt fails (e.g. due to its executor being
   * lost), then a subsequent task attempt may be authorized to commit its output.
   *
   * @param stage the stage number
   * @param partition the partition number
   * @param attemptNumber how many times this task has been attempted
   *                      (see [[TaskContext.attemptNumber()]])
   * @return true if this task is authorized to commit, false otherwise
   */
  def canCommit(
      stage: Int,
      stageAttempt: Int,
      partition: Int,
      attemptNumber: Int): Boolean = {
    val msg = AskPermissionToCommitOutput(stage, stageAttempt, partition, attemptNumber)
    coordinatorRef match {
      case Some(endpointRef) =>
        ThreadUtils.awaitResult(endpointRef.ask[Boolean](msg),
          RpcUtils.askRpcTimeout(conf).duration)
      case None =>
        logError(
          "canCommit called after coordinator was stopped (is SparkEnv shutdown in progress)?")
        false
    }
  }

  /**
   * stageStart方法用于启动给定Stage的输出提交到HDFS的协调机制，
   * 其实质为创建给定Stage的对应TaskAttemptNumber数组，并将TaskAttemptNumber
   * 数组中的所有TaskAttemptNumber置为NO_AUTHORIZED_COMMITTER
   *
   * Called by the DAGScheduler when a stage starts. Initializes the stage's state if it hasn't
   * yet been initialized.
   *
   * @param stage the stage id.
   * @param maxPartitionId the maximum partition id that could appear in this stage's tasks (i.e.
   *                       the maximum possible value of `context.partitionId`).
   */
  private[scheduler] def stageStart(stage: Int, maxPartitionId: Int): Unit = synchronized {
    stageStates.get(stage) match {
      case Some(state) =>
        require(state.authorizedCommitters.length == maxPartitionId + 1)
        logInfo(s"Reusing state from previous attempt of stage $stage.")

      case _ =>
        stageStates(stage) = new StageState(maxPartitionId + 1)
    }
  }

  /**
   * stageEnd方法（见代码清单5-94）用于停止给定Stage的输出提交到HDFS的协调机制，
   * 其实质为将给定Stage及对应的TaskAttemptNumber数组从authorizedCommitters-ByStage中删除。
   * @param stage
   */
  // Called by DAGScheduler
  private[scheduler] def stageEnd(stage: Int): Unit = synchronized {
    stageStates.remove(stage)
  }

  // Called by DAGScheduler
  private[scheduler] def taskCompleted(
      stage: Int,
      stageAttempt: Int,
      partition: Int,
      attemptNumber: Int,
      reason: TaskEndReason): Unit = synchronized {
    val stageState = stageStates.getOrElse(stage, {
      logDebug(s"Ignoring task completion for completed stage")
      return
    })
    reason match {
      case Success =>
      // The task output has been committed successfully
      case _: TaskCommitDenied =>
        logInfo(s"Task was denied committing, stage: $stage.$stageAttempt, " +
          s"partition: $partition, attempt: $attemptNumber")
      case _ =>
        // Mark the attempt as failed to blacklist from future commit protocol
        val taskId = TaskIdentifier(stageAttempt, attemptNumber)
        stageState.failures.getOrElseUpdate(partition, mutable.Set()) += taskId
        if (stageState.authorizedCommitters(partition) == taskId) {
          logDebug(s"Authorized committer (attemptNumber=$attemptNumber, stage=$stage, " +
            s"partition=$partition) failed; clearing lock")
          stageState.authorizedCommitters(partition) = null
        }
    }
  }

  def stop(): Unit = synchronized {
    if (isDriver) {
      coordinatorRef.foreach(_ send StopCoordinator)
      coordinatorRef = None
      stageStates.clear()
    }
  }

  /**
   * 用于判断给定的任务尝试是否有权限将给定Stage的指定分区的数据提交到HDFS。
   *
   * @param stage
   * @param stageAttempt
   * @param partition
   * @param attemptNumber
   * @return
   */
  // Marked private[scheduler] instead of private so this can be mocked in tests
  private[scheduler] def handleAskPermissionToCommit(
      stage: Int,
      stageAttempt: Int,
      partition: Int,
      attemptNumber: Int): Boolean = synchronized {
    stageStates.get(stage) match {
      case Some(state) if attemptFailed(state, stageAttempt, partition, attemptNumber) =>
        logInfo(s"Commit denied for stage=$stage.$stageAttempt, partition=$partition: " +
          s"task attempt $attemptNumber already marked as failed.")
        false
      case Some(state) =>
        val existing = state.authorizedCommitters(partition)
        if (existing == null) {
          logDebug(s"Commit allowed for stage=$stage.$stageAttempt, partition=$partition, " +
            s"task attempt $attemptNumber")
          state.authorizedCommitters(partition) = TaskIdentifier(stageAttempt, attemptNumber)
          true
        } else {
          logDebug(s"Commit denied for stage=$stage.$stageAttempt, partition=$partition: " +
            s"already committed by $existing")
          false
        }
      case None =>
        logDebug(s"Commit denied for stage=$stage.$stageAttempt, partition=$partition: " +
          "stage already marked as completed.")
        false
    }
  }

  private def attemptFailed(
      stageState: StageState,
      stageAttempt: Int,
      partition: Int,
      attempt: Int): Boolean = synchronized {
    val failInfo = TaskIdentifier(stageAttempt, attempt)
    stageState.failures.get(partition).exists(_.contains(failInfo))
  }
}

private[spark] object OutputCommitCoordinator {

  // This endpoint is used only for RPC
  private[spark] class OutputCommitCoordinatorEndpoint(
      override val rpcEnv: RpcEnv, outputCommitCoordinator: OutputCommitCoordinator)
    extends RpcEndpoint with Logging {

    logDebug("init") // force eager creation of logger

    override def receive: PartialFunction[Any, Unit] = {
//      StopCoordinator:此消息将停止OutputCommitCoordinatorEndpoint。
      case StopCoordinator =>
        logInfo("OutputCommitCoordinator stopped!")
        stop()
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
//      AskPermissionToCommitOutput：此消息将通过OutputCommitCoordinator的handleAskPermissionToCommit方法处理，进而确认客户端是否有权限将输出提交到HDFS。
      case AskPermissionToCommitOutput(stage, stageAttempt, partition, attemptNumber) =>
        context.reply(
          outputCommitCoordinator.handleAskPermissionToCommit(stage, stageAttempt, partition,
            attemptNumber))
    }
  }
}
