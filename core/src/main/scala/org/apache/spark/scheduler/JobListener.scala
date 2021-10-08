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

/**
 * 定义了所有Job监听器的接口规范。
 *
 * Interface used to listen for job completion or failure events after submitting a job to the
 * DAGScheduler. The listener is notified each time a task succeeds, as well as if the whole
 * job fails (and no further taskSucceeded events will happen).
 */
private[spark] trait JobListener {
  /**
   * Job执行成功后将调用JobListener定义的taskSucceeded方法
   *
   * @param index
   * @param result
   */
  def taskSucceeded(index: Int, result: Any): Unit

  /**
   * 而在Job失败后调用Job Listener定义的jobFailed方法。
   *
   * @param exception
   */
  def jobFailed(exception: Exception): Unit
}
