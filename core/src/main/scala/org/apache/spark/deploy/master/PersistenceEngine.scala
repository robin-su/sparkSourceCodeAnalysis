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

import scala.reflect.ClassTag

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rpc.RpcEnv

/**
 * PersistenceEngine用于当Master发生故障后，通过领导选举选择其他Master接替整个集群的管理工作时，能够使得新激活的Master有能力
 * 从故障中恢复整个集群的状态信息，进而恢复对集群资源的管理和分配。抽象类PersistenceEngine定义了对Master必需的任何状态信息进行
 * 持久化的接口规范
 *
 * 允许Master保留从故障中恢复所必需的任何状态。需要以下语义：
   在完成新应用程序/工作程序的注册之前，将调用addApplication和addWorker。
   可随时调用removeApplication和removeWorker。鉴于这两个要求，我们将保留所有应用程序和工作程序，
   但可能尚未删除已完成的应用程序或工作程序（因此必须在恢复过程中验证其活动性）。
   此特征的实现定义了如何存储或检索名称-对象对。
 * Allows Master to persist any state that is necessary in order to recover from a failure.
 * The following semantics are required:
 *   - addApplication and addWorker are called before completing registration of a new app/worker.
 *   - removeApplication and removeWorker are called at any time.
 * Given these two requirements, we will have all apps and workers persisted, but
 * we might not have yet deleted apps or workers that finished (so their liveness must be verified
 * during recovery).
 *
 * The implementation of this trait defines how name-object pairs are stored or retrieved.
 */
@DeveloperApi
abstract class PersistenceEngine {

  /**
   * 对对象进行序列化和持久化的抽象方法，具体的实现依赖于底层使用的存储。
   *
   * Defines how the object is serialized and persisted. Implementation will
   * depend on the store used.
   */
  def persist(name: String, obj: Object): Unit

  /**
   * 对对象进行非持久化（从存储中删除）的抽象方法，具体的实现依赖于底层使用的存储。
   *
   * Defines how the object referred by its name is removed from the store.
   */
  def unpersist(name: String): Unit

  /**
   * 读取匹配给定前缀的所有对象。
   *
   * Gives all objects, matching a prefix. This defines how objects are
   * read/deserialized back.
   */
  def read[T: ClassTag](prefix: String): Seq[T]

  /**
   * 在完成新的Application的注册之前，addApplication方法必须被调用。
   *
   * @param app
   */
  final def addApplication(app: ApplicationInfo): Unit = {
    persist("app_" + app.id, app)
  }

  /**
   * removeApplication方法可以在任何时候调用。
   * @param app
   */
  final def removeApplication(app: ApplicationInfo): Unit = {
    unpersist("app_" + app.id)
  }

  /**
   * 在完成新的Worker的注册之前，addWorker方法必须被调用。
   *
   * @param worker
   */
  final def addWorker(worker: WorkerInfo): Unit = {
    persist("worker_" + worker.id, worker)
  }

  /**
   * removeWorker方法可以在任何时候调用。
   * @param worker
   */
  final def removeWorker(worker: WorkerInfo): Unit = {
    unpersist("worker_" + worker.id)
  }

  /**
   * 对Driver的信息（DriverInfo）进行持久化的模板方法，其依赖于persist的具体实现。
   * @param driver
   */
  final def addDriver(driver: DriverInfo): Unit = {
    persist("driver_" + driver.id, driver)
  }

  /**
   * 对Driver的信息（DriverInfo）进行非持久化的模板方法，其依赖于unpersist的具体实现
   * @param driver
   */
  final def removeDriver(driver: DriverInfo): Unit = {
    unpersist("driver_" + driver.id)
  }

  /**读取并反序列化所有持久化的数据
   * Returns the persisted data sorted by their respective ids (which implies that they're
   * sorted by time of creation).
   */
  final def readPersistedData(
      rpcEnv: RpcEnv): (Seq[ApplicationInfo], Seq[DriverInfo], Seq[WorkerInfo]) = {
    rpcEnv.deserialize { () =>
      (read[ApplicationInfo]("app_"), read[DriverInfo]("driver_"), read[WorkerInfo]("worker_"))
    }
  }

  def close() {}
}

private[master] class BlackHolePersistenceEngine extends PersistenceEngine {

  override def persist(name: String, obj: Object): Unit = {}

  override def unpersist(name: String): Unit = {}

  override def read[T: ClassTag](name: String): Seq[T] = Nil

}
