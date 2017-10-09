/*
 * Copyright 2017 47 Degrees, LLC. <http://www.47deg.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package freestyle.cassandra
package util

import java.io.File

import org.apache.cassandra.config.Config.RequestSchedulerId
import org.apache.cassandra.config.{Config, ConfigurationLoader, SeedProviderDef}
import org.apache.cassandra.dht.ByteOrderedPartitioner
import org.apache.cassandra.locator.{SimpleSeedProvider, SimpleSnitch}
import org.apache.cassandra.scheduler.RoundRobinScheduler

import scala.collection.JavaConverters._

class CassandraConfigurationLoader extends ConfigurationLoader {

  import CassandraConfigurationValues._

  override def loadConfig(): Config = {
    val config = new Config
    config.cluster_name = clusterName
    config.listen_address = listenAddress
    config.start_native_transport = true
    config.native_transport_port = nativePort
    config.data_file_directories = Array[String](baseDir.getAbsolutePath)
    config.commitlog_directory = new File(baseDir, commitlogDirName).getAbsolutePath
    config.saved_caches_directory = new File(baseDir, savedCachesDirName).getAbsolutePath
    config.partitioner = classOf[ByteOrderedPartitioner].getCanonicalName
    config.endpoint_snitch = classOf[SimpleSnitch].getCanonicalName
    config.request_scheduler = classOf[RoundRobinScheduler].getCanonicalName
    config.request_scheduler_id = RequestSchedulerId.keyspace
    config.commitlog_sync_batch_window_in_ms = 1.0
    config.commitlog_sync = org.apache.cassandra.config.Config.CommitLogSync.batch
    val linkedHashMap = new java.util.LinkedHashMap[String, AnyRef]()
    linkedHashMap.put("class_name", classOf[SimpleSeedProvider].getName)
    linkedHashMap.put("parameters", List(Map("seeds" -> "127.0.0.1").asJava).asJava)
    config.seed_provider = new SeedProviderDef(linkedHashMap)
    config.concurrent_compactors = 4
    config.row_cache_size_in_mb = 64
    config
  }

}

object CassandraConfigurationValues {

  val clusterName        = "Test Cluster"
  val baseDir            = new File("target/cassandra")
  val storageDir         = new File(baseDir, "storage")
  val listenAddress      = "127.0.0.1"
  val nativePort         = 9042
  val commitlogDirName   = "commitlog"
  val hintsDirName       = "hints"
  val savedCachesDirName = "saved_caches"

}
