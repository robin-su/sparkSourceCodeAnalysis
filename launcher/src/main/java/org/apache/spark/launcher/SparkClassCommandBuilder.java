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

package org.apache.spark.launcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.spark.launcher.CommandBuilderUtils.*;

/**
 * Command builder for internal Spark classes.
 * <p>
 * This class handles building the command to launch all internal Spark classes except for
 * SparkSubmit (which is handled by {@link SparkSubmitCommandBuilder} class.
 */

// 构建class命令对象,将class和需要的参数放入对象中
class SparkClassCommandBuilder extends AbstractCommandBuilder {

  // 这里：classname=org.apache.spark.deploy.master.Master
  // classArgs=--host --port 7077 --webui-port 8080
  private final String className;
  private final List<String> classArgs;

  SparkClassCommandBuilder(String className, List<String> classArgs) {
    this.className = className;
    this.classArgs = classArgs;
  }

  // 这是构建执行命令的方法
  @Override
  public List<String> buildCommand(Map<String, String> env)
      throws IOException, IllegalArgumentException {
    List<String> javaOptsKeys = new ArrayList<>();
    String memKey = null;
    String extraClassPath = null;

    // 提交的以下class会由这个类解析为对象
    // Master, Worker, HistoryServer, ExternalShuffleService, MesosClusterDispatcher use
    // SPARK_DAEMON_JAVA_OPTS (and specific opts) + SPARK_DAEMON_MEMORY.
    switch (className) {
      case "org.apache.spark.deploy.master.Master":
        // java类: ../bin/java
        javaOptsKeys.add("SPARK_DAEMON_JAVA_OPTS");
        // spark类和参数: org.apache.spark.deploy.master.Master --host --port 7077 --webui-port 8080
        javaOptsKeys.add("SPARK_MASTER_OPTS");
        // 依赖jar包: ../jars/*
        extraClassPath = getenv("SPARK_DAEMON_CLASSPATH");
        // 执行内存
        memKey = "SPARK_DAEMON_MEMORY";
        break;
      case "org.apache.spark.deploy.worker.Worker":
        javaOptsKeys.add("SPARK_DAEMON_JAVA_OPTS");
        javaOptsKeys.add("SPARK_WORKER_OPTS");
        extraClassPath = getenv("SPARK_DAEMON_CLASSPATH");
        memKey = "SPARK_DAEMON_MEMORY";
        break;
      case "org.apache.spark.deploy.history.HistoryServer":
        javaOptsKeys.add("SPARK_DAEMON_JAVA_OPTS");
        javaOptsKeys.add("SPARK_HISTORY_OPTS");
        extraClassPath = getenv("SPARK_DAEMON_CLASSPATH");
        memKey = "SPARK_DAEMON_MEMORY";
        break;
      case "org.apache.spark.executor.CoarseGrainedExecutorBackend":
        javaOptsKeys.add("SPARK_EXECUTOR_OPTS");
        memKey = "SPARK_EXECUTOR_MEMORY";
        extraClassPath = getenv("SPARK_EXECUTOR_CLASSPATH");
        break;
      case "org.apache.spark.executor.MesosExecutorBackend":
        javaOptsKeys.add("SPARK_EXECUTOR_OPTS");
        memKey = "SPARK_EXECUTOR_MEMORY";
        extraClassPath = getenv("SPARK_EXECUTOR_CLASSPATH");
        break;
      case "org.apache.spark.deploy.mesos.MesosClusterDispatcher":
        javaOptsKeys.add("SPARK_DAEMON_JAVA_OPTS");
        extraClassPath = getenv("SPARK_DAEMON_CLASSPATH");
        memKey = "SPARK_DAEMON_MEMORY";
        break;
      case "org.apache.spark.deploy.ExternalShuffleService":
      case "org.apache.spark.deploy.mesos.MesosExternalShuffleService":
        javaOptsKeys.add("SPARK_DAEMON_JAVA_OPTS");
        javaOptsKeys.add("SPARK_SHUFFLE_OPTS");
        extraClassPath = getenv("SPARK_DAEMON_CLASSPATH");
        memKey = "SPARK_DAEMON_MEMORY";
        break;
      default:
        // 会默认设置为driver内存,默认1G
        memKey = "SPARK_DRIVER_MEMORY";
        break;
    }


    // 这里构建了Java执行命令，也就是： .../java -cp
    List<String> cmd = buildJavaCommand(extraClassPath);

    for (String key : javaOptsKeys) {
      String envValue = System.getenv(key);
      if (!isEmpty(envValue) && envValue.contains("Xmx")) {
        String msg = String.format("%s is not allowed to specify max heap(Xmx) memory settings " +
                "(was %s). Use the corresponding configuration instead.", key, envValue);
        throw new IllegalArgumentException(msg);
      }
      addOptionString(cmd, envValue);
    }

    // 如果driver内存没有设置,这里再设置为前面设置的128m,添加到执行命令中: -Xmx1G
    // 最终在这里把所有配置和参数加入到执行命令中,完成了执行命令的构建
    String mem = firstNonEmpty(memKey != null ? System.getenv(memKey) : null, DEFAULT_MEM);
    cmd.add("-Xmx" + mem);
    cmd.add(className);
    cmd.addAll(classArgs);

    // 最后将构建的命令返回给spark-class执行
    return cmd;
  }

}
