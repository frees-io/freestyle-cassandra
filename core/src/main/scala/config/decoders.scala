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

import java.util.concurrent.Executor

import classy.{Decoder, Read}
import classy.config._
import com.datastax.driver.core.ProtocolOptions.Compression
import com.datastax.driver.core._
import com.datastax.driver.core.policies._
import com.typesafe.config.Config

import collection.JavaConverters._

object decoders {

  def builderCustomProp[Builder, T](b: Builder, attr: String)(f: (Builder, T) => Builder)(
      implicit r: Read[Config, T]): ConfigDecoder[Builder] =
    builderProp[Builder, T](b, attr, b => f(b, _))

  def builderProp[Builder, T](b: Builder, attr: String, f: Builder => (T => Builder))(
      implicit r: Read[Config, T]): ConfigDecoder[Builder] =
    readConfig[Option[T]](attr).map {
      case Some(v) => f(b)(v)
      case _       => b
    }

  import config.implicits._
  import config.implicits.datastax._

  abstract class DecoderBuilder[Builder] {

    def empty: Builder

    def fields: List[(Builder) => ConfigDecoder[Builder]]

    def customField[T](b: Builder, attr: String)(f: (Builder, T) => Builder)(
        implicit r: Read[Config, T]): ConfigDecoder[Builder] =
      builderCustomProp[Builder, T](b, attr)(f)

    def field[T](b: Builder, attr: String, f: Builder => (T => Builder))(
        implicit r: Read[Config, T]): ConfigDecoder[Builder] = builderProp[Builder, T](b, attr, f)

    def flagField(b: Builder, attr: String, f: Builder => Builder): ConfigDecoder[Builder] =
      customField[Boolean](b, attr) {
        case (builder, true) => f(builder)
        case (builder, _)    => builder
      }

    def build: ConfigDecoder[Builder] =
      fields.foldLeft[ConfigDecoder[Builder]](Decoder.const(empty)) { (d, b) =>
        d.flatMap(b)
      }

  }

  class PoolingOptionsBuilder extends DecoderBuilder[PoolingOptions] {

    override def empty: PoolingOptions = new PoolingOptions

    override def fields: List[(PoolingOptions) => ConfigDecoder[PoolingOptions]] = List(
      customField[ConnectionsPerHost](_, "connectionsPerHost") { (b, v) =>
        b.setConnectionsPerHost(v.distance, v.core, v.max)
      },
      customField[CoreConnectionsPerHost](_, "coreConnectionsPerHost") { (b, v) =>
        b.setCoreConnectionsPerHost(v.distance, v.newCoreConnections)
      },
      customField[MaxConnectionsPerHost](_, "maxConnectionsPerHost") { (b, v) =>
        b.setMaxConnectionsPerHost(v.distance, v.newMaxConnections)
      },
      customField[NewConnectionThreshold](_, "newConnectionThreshold") { (b, v) =>
        b.setNewConnectionThreshold(v.distance, v.newValue)
      },
      field[Int](_, "heartbeatIntervalSeconds", _.setHeartbeatIntervalSeconds),
      field[Int](_, "idleTimeoutSeconds", _.setIdleTimeoutSeconds),
      field[Int](_, "maxQueueSize", _.setMaxQueueSize),
      field[Int](_, "poolTimeoutMillis", _.setPoolTimeoutMillis),
      field[Executor](_, "initializationExecutor", _.setInitializationExecutor)
    )
  }

  class QueryOptionsBuilder extends DecoderBuilder[QueryOptions] {

    override def empty: QueryOptions = new QueryOptions

    override def fields: List[(QueryOptions) => ConfigDecoder[QueryOptions]] = List(
      field[ConsistencyLevel](_, "consistencyLevel", _.setConsistencyLevel),
      field[ConsistencyLevel](_, "serialConsistencyLevel", _.setSerialConsistencyLevel),
      field[Boolean](_, "defaultIdempotence", _.setDefaultIdempotence),
      field[Int](_, "fetchSize", _.setFetchSize),
      field[Int](_, "maxPendingRefreshNodeListRequests", _.setMaxPendingRefreshNodeListRequests),
      field[Int](_, "maxPendingRefreshNodeRequests", _.setMaxPendingRefreshNodeRequests),
      field[Int](_, "maxPendingRefreshSchemaRequests", _.setMaxPendingRefreshSchemaRequests),
      field[Boolean](_, "metadataEnabled", _.setMetadataEnabled),
      field[Boolean](_, "prepareOnAllHosts", _.setPrepareOnAllHosts),
      field[Int](_, "refreshNodeIntervalMillis", _.setRefreshNodeIntervalMillis),
      field[Int](_, "refreshNodeListIntervalMillis", _.setRefreshNodeListIntervalMillis),
      field[Int](_, "refreshSchemaIntervalMillis", _.setRefreshSchemaIntervalMillis),
      field[Boolean](_, "reprepareOnUp", _.setReprepareOnUp)
    )
  }

  class SocketOptionsBuilder extends DecoderBuilder[SocketOptions] {
    override def empty: SocketOptions = new SocketOptions

    override def fields: List[(SocketOptions) => ConfigDecoder[SocketOptions]] = List(
      field[Int](_, "connectTimeoutMillis", _.setConnectTimeoutMillis),
      field[Boolean](_, "keepAlive", _.setKeepAlive),
      field[Int](_, "readTimeoutMillis", _.setReadTimeoutMillis),
      field[Int](_, "receiveBufferSize", _.setReceiveBufferSize),
      field[Int](_, "sendBufferSize", _.setSendBufferSize),
      field[Boolean](_, "reuseAddress", _.setReuseAddress),
      field[Int](_, "soLinger", _.setSoLinger),
      field[Boolean](_, "tcpNoDelay", _.setTcpNoDelay)
    )
  }

  class ClusterDecoderBuilder extends DecoderBuilder[Cluster.Builder] {
    override def empty: Cluster.Builder = new Cluster.Builder

    override def fields: List[(Cluster.Builder) => ConfigDecoder[Cluster.Builder]] = List(
      customField[ContactPoints](_, "contactPoints") {
        case (b, ContactPointList(l))         => b.addContactPoints(l.asJava)
        case (b, ContactPointWithPortList(l)) => b.addContactPointsWithPorts(l.asJava)
      },
      customField[Credentials](_, "credentials") { (b, v) =>
        b.withCredentials(v.username, v.password)
      },
      flagField(_, "allowBetaProtocolVersion", _.allowBetaProtocolVersion),
      flagField(_, "enableSSL", _.withSSL),
      field[String](_, "name", _.withClusterName),
      field[AddressTranslator](_, "addressTranslator", _.withAddressTranslator),
      field[AuthProvider](_, "authProvider", _.withAuthProvider),
      field[LoadBalancingPolicy](_, "loadBalancingPolicy", _.withLoadBalancingPolicy),
      field[ReconnectionPolicy](_, "reconnectionPolicy", _.withReconnectionPolicy),
      field[RetryPolicy](_, "retryPolicy", _.withRetryPolicy),
      field[SpeculativeExecutionPolicy](
        _,
        "speculativeExecutionPolicy",
        _.withSpeculativeExecutionPolicy),
      field[SSLOptions](_, "sslOptions", _.withSSL),
      field[ThreadingOptions](_, "threadingOptions", _.withThreadingOptions),
      field[TimestampGenerator](_, "timestampGenerator", _.withTimestampGenerator),
      field[Int](_, "maxSchemaAgreementWaitSeconds", _.withMaxSchemaAgreementWaitSeconds),
      field[Int](_, "port", _.withPort),
      field[Compression](_, "compression", _.withCompression),
      field[ProtocolVersion](_, "protocolVersion", _.withProtocolVersion),
      field[PoolingOptions](_, "poolingOptions", _.withPoolingOptions),
      field[QueryOptions](_, "queryOptions", _.withQueryOptions),
      field[SocketOptions](_, "socketOptions", _.withSocketOptions)
    )
  }

  implicit val poolingOptionsDecoder: ConfigDecoder[PoolingOptions] =
    new PoolingOptionsBuilder().build

  implicit val queryOptionsDecoder: ConfigDecoder[QueryOptions] =
    new QueryOptionsBuilder().build

  implicit val socketOptionsDecoder: ConfigDecoder[SocketOptions] =
    new SocketOptionsBuilder().build

  implicit val clusterBuilderDecoder: ConfigDecoder[Cluster.Builder] =
    new ClusterDecoderBuilder().build

}
