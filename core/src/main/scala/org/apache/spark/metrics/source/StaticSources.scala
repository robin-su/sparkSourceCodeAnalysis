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

package org.apache.spark.metrics.source

import com.codahale.metrics.MetricRegistry

import org.apache.spark.annotation.Experimental

private[spark] object StaticSources {
  /**
   *
   * 将静态的度量来源CodegenMetrics和HiveCatalogMetrics注册到MetricRegistry
   *
   * The set of all static sources. These sources may be reported to from any class, including
   * static classes, without requiring reference to a SparkEnv.
   */
  val allSouStaticSourcesrces = Seq(CodegenMetrics, HiveCatalogMetrics)
}

/**
 * :: Experimental ::
 * Metrics for code generation.
 */
@Experimental
object CodegenMetrics extends Source {
  override val sourceName: String = "CodeGenerator"
  override val metricRegistry: MetricRegistry = new MetricRegistry()

  /**
   * Histogram of the length of source code text compiled by CodeGenerator (in characters).
   */
  val METRIC_SOURCE_CODE_SIZE = metricRegistry.histogram(MetricRegistry.name("sourceCodeSize"))

  /**
   * Histogram of the time it took to compile source code text (in milliseconds).
   */
  val METRIC_COMPILATION_TIME = metricRegistry.histogram(MetricRegistry.name("compilationTime"))

  /**
   * Histogram of the bytecode size of each class generated by CodeGenerator.
   */
  val METRIC_GENERATED_CLASS_BYTECODE_SIZE =
    metricRegistry.histogram(MetricRegistry.name("generatedClassSize"))

  /**
   * Histogram of the bytecode size of each method in classes generated by CodeGenerator.
   */
  val METRIC_GENERATED_METHOD_BYTECODE_SIZE =
    metricRegistry.histogram(MetricRegistry.name("generatedMethodSize"))
}

/**
 * :: Experimental ::
 * Metrics for access to the hive external catalog.
 */
@Experimental
object HiveCatalogMetrics extends Source {
  override val sourceName: String = "HiveExternalCatalog"
  override val metricRegistry: MetricRegistry = new MetricRegistry()

  /**
   * Tracks the total number of partition metadata entries fetched via the client api.
   */
  val METRIC_PARTITIONS_FETCHED = metricRegistry.counter(MetricRegistry.name("partitionsFetched"))

  /**
   * Tracks the total number of files discovered off of the filesystem by InMemoryFileIndex.
   */
  val METRIC_FILES_DISCOVERED = metricRegistry.counter(MetricRegistry.name("filesDiscovered"))

  /**
   * Tracks the total number of files served from the file status cache instead of discovered.
   */
  val METRIC_FILE_CACHE_HITS = metricRegistry.counter(MetricRegistry.name("fileCacheHits"))

  /**
   * Tracks the total number of Hive client calls (e.g. to lookup a table).
   */
  val METRIC_HIVE_CLIENT_CALLS = metricRegistry.counter(MetricRegistry.name("hiveClientCalls"))

  /**
   * Tracks the total number of Spark jobs launched for parallel file listing.
   */
  val METRIC_PARALLEL_LISTING_JOB_COUNT = metricRegistry.counter(
    MetricRegistry.name("parallelListingJobCount"))

  /**
   * Resets the values of all metrics to zero. This is useful in tests.
   */
  def reset(): Unit = {
    METRIC_PARTITIONS_FETCHED.dec(METRIC_PARTITIONS_FETCHED.getCount())
    METRIC_FILES_DISCOVERED.dec(METRIC_FILES_DISCOVERED.getCount())
    METRIC_FILE_CACHE_HITS.dec(METRIC_FILE_CACHE_HITS.getCount())
    METRIC_HIVE_CLIENT_CALLS.dec(METRIC_HIVE_CLIENT_CALLS.getCount())
    METRIC_PARALLEL_LISTING_JOB_COUNT.dec(METRIC_PARALLEL_LISTING_JOB_COUNT.getCount())
  }

  // clients can use these to avoid classloader issues with the codahale classes
  def incrementFetchedPartitions(n: Int): Unit = METRIC_PARTITIONS_FETCHED.inc(n)
  def incrementFilesDiscovered(n: Int): Unit = METRIC_FILES_DISCOVERED.inc(n)
  def incrementFileCacheHits(n: Int): Unit = METRIC_FILE_CACHE_HITS.inc(n)
  def incrementHiveClientCalls(n: Int): Unit = METRIC_HIVE_CLIENT_CALLS.inc(n)
  def incrementParallelListingJobCount(n: Int): Unit = METRIC_PARALLEL_LISTING_JOB_COUNT.inc(n)
}
