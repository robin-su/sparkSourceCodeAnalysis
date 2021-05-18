#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Starts the master on the machine this script is executed on.

if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

# NOTE: This exact class name is matched downstream by SparkSubmit. 这个主类名用于下游sparksubmit匹配
# Any changes need to be reflected there. 设置CLASS变量为master类
CLASS="org.apache.spark.deploy.master.Master"

if [[ "$@" = *--help ]] || [[ "$@" = *-h ]]; then
  echo "Usage: ./sbin/start-master.sh [options]"
  pattern="Usage:"
  pattern+="\|Using Spark's default log4j profile:"
  pattern+="\|Registered signal handlers for"

  # 打印帮助信息
  # 加载spark-class,再执行launch/main,见下面spark-class中build_command方法
  # 传入参数.master类，调用--help方法打印帮助信息
  # 将错误信息重定向到标准输出,过滤含有pattern的字符串
  # 完整：spark-class org.apache.spark.deploy.master.Master --help
  "${SPARK_HOME}"/bin/spark-class $CLASS --help 2>&1 | grep -v "$pattern" 1>&2
  exit 1
fi


# 将调用start-master的参数列表赋值给ORIGINAL_ARGS
# 从start-all.sh传过的,来没有参数
ORIGINAL_ARGS="$@"

. "${SPARK_HOME}/sbin/spark-config.sh" # 同样加载conf目录为环境变量

. "${SPARK_HOME}/bin/load-spark-env.sh"  # 启动加载spark-env的脚本

if [ "$SPARK_MASTER_PORT" = "" ]; then # 如果master端口为空,设置默认为7077
  SPARK_MASTER_PORT=7077
fi

if [ "$SPARK_MASTER_HOST" = "" ]; then # 设置master的host,即当前脚本运行主机名
  case `uname` in # 匹配hostname,lunix下查看hostname命令为uname
      (SunOS)     # 如果hostname为SunOs,设置host为查看hostname的最后一个字段
	  SPARK_MASTER_HOST="`/usr/sbin/check-hostname | awk '{print $NF}'`"
	  ;; # 匹配中断
      (*)  # 如果hostname为其他,设置为hostname -f查看的结果
	  SPARK_MASTER_HOST="`hostname -f`"
	  ;;
  esac
fi

# 如果webUI端口为空,设置默认为8080
if [ "$SPARK_MASTER_WEBUI_PORT" = "" ]; then
  SPARK_MASTER_WEBUI_PORT=8080
fi


# 启动spark-daemon脚本,参数为：start、$CLASS、1、host、port、webUI-port、$ORIGINAL_ARGS
# 直译为:
# sbin/spark-daemon.sh start org.apache.spark.deploy.master.Master 1
# --host hostname --port 7077 --webui-port 8080
"${SPARK_HOME}/sbin"/spark-daemon.sh start $CLASS 1 \
  --host $SPARK_MASTER_HOST --port $SPARK_MASTER_PORT --webui-port $SPARK_MASTER_WEBUI_PORT \
  $ORIGINAL_ARGS
