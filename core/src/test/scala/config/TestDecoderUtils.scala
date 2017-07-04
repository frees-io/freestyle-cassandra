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
package config

import com.datastax.driver.core.{PoolingOptions, QueryOptions, SocketOptions}
import freestyle.cassandra.config.ClusterConfig.{
  PoolingOptionsConfig,
  QueryOptionsConfig,
  SocketOptionsConfig
}
import org.scalamock.scalatest.MockFactory
import org.scalatest.prop.Checkers
import org.scalatest.{Matchers, WordSpec}

class TestDecoderUtils extends WordSpec with Matchers with Checkers with MockFactory {

  import classy.config._
  import com.typesafe.config.Config

  val decoders = new Decoders[Config]
  import decoders._

  val executorClass = "freestyle.cassandra.config.MyJavaExecutor"

  def preparePoolingOptionsDecoder(
      c: PoolingOptionsConfig): (PoolingOptionsBuilder, PoolingOptionsConfig) = {
    val poc =
      c.copy(initializationExecutor = c.initializationExecutor.map(_ => executorClass))
    val poolingOptionsMock: PoolingOptions = mock[PoolingOptions]
    poc.connectionsPerHost.foreach { v =>
      (poolingOptionsMock.setConnectionsPerHost _)
        .expects(v.distance, v.code, v.max)
        .returns(poolingOptionsMock)
    }
    poc.coreConnectionsPerHost.foreach { v =>
      (poolingOptionsMock.setCoreConnectionsPerHost _)
        .expects(v.distance, v.newCoreConnections)
        .returns(poolingOptionsMock)
    }
    poc.heartbeatIntervalSeconds.foreach { v =>
      (poolingOptionsMock.setHeartbeatIntervalSeconds _)
        .expects(v)
        .returns(poolingOptionsMock)
    }
    poc.idleTimeoutSeconds.foreach { v =>
      (poolingOptionsMock.setIdleTimeoutSeconds _)
        .expects(v)
        .returns(poolingOptionsMock)
    }
    poc.maxConnectionsPerHost.foreach { v =>
      (poolingOptionsMock.setMaxConnectionsPerHost _)
        .expects(v.distance, v.maxCoreConnections)
        .returns(poolingOptionsMock)
    }
    poc.maxQueueSize.foreach { v =>
      (poolingOptionsMock.setMaxQueueSize _)
        .expects(v)
        .returns(poolingOptionsMock)
    }
    poc.maxRequestsPerConnection.foreach { v =>
      (poolingOptionsMock.setMaxRequestsPerConnection _)
        .expects(v.distance, v.newMaxRequests)
        .returns(poolingOptionsMock)
    }
    poc.newConnectionThreshold.foreach { v =>
      (poolingOptionsMock.setNewConnectionThreshold _)
        .expects(v.distance, v.newValue)
        .returns(poolingOptionsMock)
    }
    poc.poolTimeoutMillis.foreach { v =>
      (poolingOptionsMock.setPoolTimeoutMillis _)
        .expects(v)
        .returns(poolingOptionsMock)
    }
    poc.initializationExecutor.foreach { v =>
      (poolingOptionsMock.setInitializationExecutor _)
        .expects(where { e: java.util.concurrent.Executor =>
          e.isInstanceOf[MyJavaExecutor]
        })
        .returns(poolingOptionsMock)
    }

    val builder = new PoolingOptionsBuilder() {
      override def empty: PoolingOptions = poolingOptionsMock
    }

    (builder, poc)
  }

  def prepareQueryOptionsDecoder(
      qoc: QueryOptionsConfig): (QueryOptionsBuilder, QueryOptionsConfig) = {
    val qoMock: QueryOptions = mock[QueryOptions]
    qoc.consistencyLevel.foreach { v =>
      (qoMock.setConsistencyLevel _).expects(v.consistency).returns(qoMock)
    }
    qoc.defaultIdempotence.foreach { v =>
      (qoMock.setDefaultIdempotence _).expects(v).returns(qoMock)
    }
    qoc.fetchSize.foreach { v =>
      (qoMock.setFetchSize _).expects(v).returns(qoMock)
    }
    qoc.maxPendingRefreshNodeListRequests.foreach { v =>
      (qoMock.setMaxPendingRefreshNodeListRequests _).expects(v).returns(qoMock)
    }
    qoc.maxPendingRefreshNodeRequests.foreach { v =>
      (qoMock.setMaxPendingRefreshNodeRequests _).expects(v).returns(qoMock)
    }
    qoc.maxPendingRefreshSchemaRequests.foreach { v =>
      (qoMock.setMaxPendingRefreshSchemaRequests _).expects(v).returns(qoMock)
    }
    qoc.metadataEnabled.foreach { v =>
      (qoMock.setMetadataEnabled _).expects(v).returns(qoMock)
    }
    qoc.prepareOnAllHosts.foreach { v =>
      (qoMock.setPrepareOnAllHosts _).expects(v).returns(qoMock)
    }
    qoc.refreshNodeIntervalMillis.foreach { v =>
      (qoMock.setRefreshNodeIntervalMillis _).expects(v).returns(qoMock)
    }
    qoc.refreshNodeListIntervalMillis.foreach { v =>
      (qoMock.setRefreshNodeListIntervalMillis _).expects(v).returns(qoMock)
    }
    qoc.refreshSchemaIntervalMillis.foreach { v =>
      (qoMock.setRefreshSchemaIntervalMillis _).expects(v).returns(qoMock)
    }
    qoc.reprepareOnUp.foreach { v =>
      (qoMock.setReprepareOnUp _).expects(v).returns(qoMock)
    }
    qoc.serialConsistencyLevel.foreach { v =>
      (qoMock.setSerialConsistencyLevel _).expects(v.consistency).returns(qoMock)
    }

    val builder = new QueryOptionsBuilder() {
      override def empty: QueryOptions = qoMock
    }

    (builder, qoc)
  }

  def prepareSocketOptionsDecoder(
      soc: SocketOptionsConfig): (SocketOptionsBuilder, SocketOptionsConfig) = {
    val soMock: SocketOptions = mock[SocketOptions]
    soc.connectTimeoutMillis.foreach { v =>
      (soMock.setConnectTimeoutMillis _).expects(v).returns(soMock)
    }
    soc.keepAlive.foreach { v =>
      (soMock.setKeepAlive _).expects(v).returns(soMock)
    }
    soc.readTimeoutMillis.foreach { v =>
      (soMock.setReadTimeoutMillis _).expects(v).returns(soMock)
    }
    soc.receiveBufferSize.foreach { v =>
      (soMock.setReceiveBufferSize _).expects(v).returns(soMock)
    }
    soc.reuseAddress.foreach { v =>
      (soMock.setReuseAddress _).expects(v).returns(soMock)
    }
    soc.sendBufferSize.foreach { v =>
      (soMock.setSendBufferSize _).expects(v).returns(soMock)
    }
    soc.soLinger.foreach { v =>
      (soMock.setSoLinger _).expects(v).returns(soMock)
    }

    val builder = new SocketOptionsBuilder() {
      override def empty: SocketOptions = soMock
    }

    (builder, soc)
  }

}
