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

import java.io._

import scala.reflect.ClassTag

import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer}
import org.apache.spark.util.Utils


/**
 * FileSystemPersistenceEngine是基于文件系统的持久化引擎。对于ApplicationInfo、WorkerInfo及DriverInfo,
 * FileSystemPersistenceEngine会将它们的数据存储到磁盘上的单个文件夹中，当要移除它们时，这些磁盘文件将被删除。
 * 由于不同的Master往往不在同一个机器节点上，因此在使用FileSystemPersistenceEngine时，底层的文件系统应该是分布式的。
 *
 * Stores data in a single on-disk directory with one file per application and worker.
 * Files are deleted when applications and workers are removed.
 *
 * @param dir Directory to store files. Created if non-existent (but not recursively).
 * @param serializer Used to serialize our objects.
 */
private[master] class FileSystemPersistenceEngine(
    val dir: String, // 持久化的根目录。
    val serializer: Serializer) //持久化时使用的序列化器。
  extends PersistenceEngine with Logging {


  new File(dir).mkdir()

  override def persist(name: String, obj: Object): Unit = {
    serializeIntoFile(new File(dir + File.separator + name), obj)
  }

  override def unpersist(name: String): Unit = {
    val f = new File(dir + File.separator + name)
    if (!f.delete()) {
      logWarning(s"Error deleting ${f.getPath()}")
    }
  }

  /**
   * 实际是通过过滤出持久化根目录下，文件名匹配前缀的所有文件，并对每个文件的数据反序列化实现的。
   *
   * @param prefix
   * @tparam T
   * @return
   */
  override def read[T: ClassTag](prefix: String): Seq[T] = {
    val files = new File(dir).listFiles().filter(_.getName.startsWith(prefix))
    files.map(deserializeFromFile[T])
  }

  /**
   * persist方法实际是通过创建文件，打开文件输出流，并对数据进行序列化后写入磁盘的。
   * @param file
   * @param value
   */
  private def serializeIntoFile(file: File, value: AnyRef) {
    val created = file.createNewFile()
    if (!created) { throw new IllegalStateException("Could not create file: " + file) }
    val fileOut = new FileOutputStream(file)
    var out: SerializationStream = null
    Utils.tryWithSafeFinally {
      out = serializer.newInstance().serializeStream(fileOut)
      out.writeObject(value)
    } {
      fileOut.close()
      if (out != null) {
        out.close()
      }
    }
  }

  private def deserializeFromFile[T](file: File)(implicit m: ClassTag[T]): T = {
    val fileIn = new FileInputStream(file)
    var in: DeserializationStream = null
    try {
      in = serializer.newInstance().deserializeStream(fileIn)
      in.readObject[T]()
    } finally {
      fileIn.close()
      if (in != null) {
        in.close()
      }
    }
  }

}
