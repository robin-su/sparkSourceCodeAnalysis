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

package org.apache.spark.deploy.master

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.{LeaderLatch, LeaderLatchListener}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkCuratorUtil
import org.apache.spark.internal.Logging

/**
 * LeaderLatch实际是通过在Master上创建领导选举的Znode节点，并对Znode节点添加监视点来发现Master是否成功“竞选”的
 *
 * @param masterInstance
 * @param conf
 */
private[master] class ZooKeeperLeaderElectionAgent(val masterInstance: LeaderElectable,
    conf: SparkConf) extends LeaderLatchListener with LeaderElectionAgent with Logging  {

  /**
   * ZooKeeperLeaderElectionAgent在ZooKeeper上的工作目录，是Spark基于ZooKeeper进行热备的根节点
   * （可通过spark.deploy.ZooKeeper.dir属性配置，默认为spark）的子节点leader_election。
   */
  val WORKING_DIR = conf.get("spark.deploy.zookeeper.dir", "/spark") + "/leader_election"

  /**
   * 连接ZooKeeper的客户端，类型为CuratorFramework
   */
  private var zk: CuratorFramework = _
  /**
   * 使用ZooKeeper进行领导选举的客户端，类型为LeaderLatch。
   */
  private var leaderLatch: LeaderLatch = _
  /**
   * 领导选举的状态，包括有领导（LEADER）和无领导（NOT_LEADER）。
   */
  private var status = LeadershipStatus.NOT_LEADER
  /**
   * 启动基于ZooKeeper的领导选举代理
   */
  start()

  private def start() {
    logInfo("Starting ZooKeeper LeaderElection agent")
    zk = SparkCuratorUtil.newClient(conf)
    /**
     * LeaderLatch是Curator提供的进行主节点选举的API。
     */
    leaderLatch = new LeaderLatch(zk, WORKING_DIR)

    /**
     * 为了在Curator客户端获得或者失去管理权时进行回调处理，需要注册一个LeaderLatchListener接口的实现类。LeaderLatchListener接口中定义了isLeader和notLeader两个方法，需要实现类实现。
     */
    leaderLatch.addListener(this)
    leaderLatch.start()
  }

  override def stop() {
    leaderLatch.close()
    zk.close()
  }

  override def isLeader() {
    synchronized {
      // could have lost leadership by now.
      if (!leaderLatch.hasLeadership) {
        return
      }

      logInfo("We have gained leadership")
      updateLeadershipStatus(true)
    }
  }

  /**
   * 告知ZooKeeperLeaderElectionAgent所属的Master节点没有被选为Leader，并更新领导关系状态。
   * 当LeaderLatch发现Master没有被选举为Leader，会调用ZooKeeperLeaderElectionAgent的notLeader方法
   */
  override def notLeader() {
    synchronized {
      // could have gained leadership by now.
      if (leaderLatch.hasLeadership) {
        return
      }

      logInfo("We have lost leadership")
      updateLeadershipStatus(false)
    }
  }

  private def updateLeadershipStatus(isLeader: Boolean) {
    if (isLeader && status == LeadershipStatus.NOT_LEADER) { // 被选为领导
      status = LeadershipStatus.LEADER
      masterInstance.electedLeader()
    } else if (!isLeader && status == LeadershipStatus.LEADER) { // 撤销领导职务
      status = LeadershipStatus.NOT_LEADER
      masterInstance.revokedLeadership()
    }
  }

  private object LeadershipStatus extends Enumeration {
    type LeadershipStatus = Value
    // 不是领导，是领导
    val LEADER, NOT_LEADER = Value
  }
}
