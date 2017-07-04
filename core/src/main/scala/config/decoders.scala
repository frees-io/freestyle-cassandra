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
import config.model._
import config.reads._
import com.datastax.driver.core.ProtocolOptions.Compression
import com.datastax.driver.core._
import com.datastax.driver.core.policies._

import collection.JavaConverters._

class Decoders[Config] extends DatastaxReads[Config] {

  import reads.maps._

  def builderCustomProp[Builder, T](b: Builder, attr: String)(f: (Builder, T) => Builder)(
      implicit R: Read[Config, Option[T]]): Decoder[Config, Builder] =
    builderProp[Builder, T](b, attr, b => f(b, _))

  def builderProp[Builder, T](b: Builder, attr: String, f: Builder => (T => Builder))(
      implicit R: Read[Config, Option[T]]): Decoder[Config, Builder] =
    R(attr).map {
      case Some(v) => f(b)(v)
      case _       => b
    }

  abstract class DecoderBuilder[Builder](implicit R1: Read[Config, Boolean]) {

    type FunctionField = (Builder, Option[String]) => Decoder[Config, Builder]

    def empty: Builder

    def fields: List[FunctionField]

    def customField[T](attr: String)(f: (Builder, T) => Builder)(
        implicit R: Read[Config, Option[T]]): FunctionField =
      (b: Builder, path: Option[String]) =>
        builderCustomProp[Builder, T](b, buildPath(path, attr))(f)

    def field[T](attr: String, f: Builder => (T => Builder))(
        implicit R: Read[Config, T]): FunctionField =
      (b: Builder, path: Option[String]) => builderProp[Builder, T](b, buildPath(path, attr), f)

    def flagField(attr: String, f: Builder => Builder): FunctionField =
      customField[Boolean](attr) {
        case (builder, true) => f(builder)
        case (builder, _)    => builder
      }

    def build: Decoder[Config, Builder] =
      fields.foldLeft[Decoder[Config, Builder]](Decoder.const(empty)) { (d, f) =>
        d.flatMap(b => f(b, None))
      }

    def buildRead: Read[Config, Builder] =
      Read.instance[Config, Builder] { path =>
        fields.foldLeft[Decoder[Config, Builder]](Decoder.const(empty)) { (d, f) =>
          d.flatMap(b => f(b, Some(path)))
        }
      }

    private[this] def buildPath(path: Option[String], attr: String): String =
      path.map(p => s"$p.$attr").getOrElse(attr)

  }

  class PoolingOptionsBuilder(
      implicit R1: Read[Config, String],
      R2: Read[Config, Int],
      R3: Read[Config, Boolean])
      extends DecoderBuilder[PoolingOptions] {

    override def empty: PoolingOptions = new PoolingOptions

    override def fields: List[FunctionField] =
      List(
        customField[ConnectionsPerHost]("connectionsPerHost") { (b, v) =>
          b.setConnectionsPerHost(v.distance, v.core, v.max)
        }(connectionsPerHostDecoder),
        customField[CoreConnectionsPerHost]("coreConnectionsPerHost") { (b, v) =>
          b.setCoreConnectionsPerHost(v.distance, v.newCoreConnections)
        }(coreConnectionsPerHostDecoder),
        customField[MaxConnectionsPerHost]("maxConnectionsPerHost") { (b, v) =>
          b.setMaxConnectionsPerHost(v.distance, v.newMaxConnections)
        }(maxConnectionsPerHostDecoder),
        customField[MaxRequestsPerConnection]("maxRequestsPerConnection") { (b, v) =>
          b.setMaxRequestsPerConnection(v.distance, v.newMaxRequests)
        }(maxRequestsPerConnectionDecoder),
        customField[NewConnectionThreshold]("newConnectionThreshold") { (b, v) =>
          b.setNewConnectionThreshold(v.distance, v.newValue)
        }(newConnectionThresholdDecoder),
        field[Int]("heartbeatIntervalSeconds", _.setHeartbeatIntervalSeconds),
        field[Int]("idleTimeoutSeconds", _.setIdleTimeoutSeconds),
        field[Int]("maxQueueSize", _.setMaxQueueSize),
        field[Int]("poolTimeoutMillis", _.setPoolTimeoutMillis),
        field[Executor]("initializationExecutor", _.setInitializationExecutor)(javaExecutorRead)
      )
  }

  class QueryOptionsBuilder(
      implicit R1: Read[Config, String],
      R2: Read[Config, Int],
      R3: Read[Config, Boolean])
      extends DecoderBuilder[QueryOptions] {

    override def empty: QueryOptions = new QueryOptions

    override def fields: List[FunctionField] =
      List(
        field[ConsistencyLevel]("consistencyLevel", _.setConsistencyLevel)(
          stringListRead[ConsistencyLevel]),
        field[ConsistencyLevel]("serialConsistencyLevel", _.setSerialConsistencyLevel)(
          stringListRead[ConsistencyLevel]),
        field[Boolean]("defaultIdempotence", _.setDefaultIdempotence),
        field[Int]("fetchSize", _.setFetchSize),
        field[Int]("maxPendingRefreshNodeListRequests", _.setMaxPendingRefreshNodeListRequests),
        field[Int]("maxPendingRefreshNodeRequests", _.setMaxPendingRefreshNodeRequests),
        field[Int]("maxPendingRefreshSchemaRequests", _.setMaxPendingRefreshSchemaRequests),
        field[Boolean]("metadataEnabled", _.setMetadataEnabled),
        field[Boolean]("prepareOnAllHosts", _.setPrepareOnAllHosts),
        field[Int]("refreshNodeIntervalMillis", _.setRefreshNodeIntervalMillis),
        field[Int]("refreshNodeListIntervalMillis", _.setRefreshNodeListIntervalMillis),
        field[Int]("refreshSchemaIntervalMillis", _.setRefreshSchemaIntervalMillis),
        field[Boolean]("reprepareOnUp", _.setReprepareOnUp)
      )
  }

  class SocketOptionsBuilder(implicit R1: Read[Config, Int], R2: Read[Config, Boolean])
      extends DecoderBuilder[SocketOptions] {
    override def empty: SocketOptions = new SocketOptions

    override def fields: List[FunctionField] =
      List(
        field[Int]("connectTimeoutMillis", _.setConnectTimeoutMillis),
        field[Boolean]("keepAlive", _.setKeepAlive),
        field[Int]("readTimeoutMillis", _.setReadTimeoutMillis),
        field[Int]("receiveBufferSize", _.setReceiveBufferSize),
        field[Int]("sendBufferSize", _.setSendBufferSize),
        field[Boolean]("reuseAddress", _.setReuseAddress),
        field[Int]("soLinger", _.setSoLinger),
        field[Boolean]("tcpNoDelay", _.setTcpNoDelay)
      )
  }

  class ClusterDecoderBuilder(
      implicit R1: Read[Config, String],
      R2: Read[Config, Int],
      R3: Read[Config, Boolean],
      R4: Read[Config, List[String]])
      extends DecoderBuilder[Cluster.Builder] {
    override def empty: Cluster.Builder = new Cluster.Builder

    val poolingOptionsRead: Read[Config, PoolingOptions] = new PoolingOptionsBuilder().buildRead
    val queryOptionsRead: Read[Config, QueryOptions]     = new QueryOptionsBuilder().buildRead
    val socketOptionsRead: Read[Config, SocketOptions]   = new SocketOptionsBuilder().buildRead

    override def fields: List[FunctionField] = List(
      customField[ContactPoints]("contactPoints") {
        case (b, ContactPointList(l))         => b.addContactPoints(l.asJava)
        case (b, ContactPointWithPortList(l)) => b.addContactPointsWithPorts(l.asJava)
      }(contactPointListRead),
      customField[Credentials]("credentials") { (b, v) =>
        b.withCredentials(v.username, v.password)
      }(credentialsDecoder),
      flagField("allowBetaProtocolVersion", _.allowBetaProtocolVersion),
      flagField("enableSSL", _.withSSL),
      field[String]("name", _.withClusterName),
      field[AddressTranslator]("addressTranslator", _.withAddressTranslator)(
        addressTranslatorRead),
      field[AuthProvider]("authProvider", _.withAuthProvider)(authProviderRead),
      field[LoadBalancingPolicy]("loadBalancingPolicy", _.withLoadBalancingPolicy)(
        loadBalancingRead),
      field[ReconnectionPolicy]("reconnectionPolicy", _.withReconnectionPolicy)(
        reconnectionPolicyRead),
      field[RetryPolicy]("retryPolicy", _.withRetryPolicy)(retryPolicyRead),
      field[SpeculativeExecutionPolicy](
        "speculativeExecutionPolicy",
        _.withSpeculativeExecutionPolicy)(executionPolicyRead),
      field[SSLOptions]("sslOptions", _.withSSL)(sslOptionsRead),
      field[ThreadingOptions]("threadingOptions", _.withThreadingOptions)(threadingOptionsRead),
      field[TimestampGenerator]("timestampGenerator", _.withTimestampGenerator)(
        timestampGeneratorRead),
      field[Int]("maxSchemaAgreementWaitSeconds", _.withMaxSchemaAgreementWaitSeconds),
      field[Int]("port", _.withPort),
      field[Compression]("compression", _.withCompression)(stringListRead[Compression]),
      field[ProtocolVersion]("protocolVersion", _.withProtocolVersion)(
        stringListRead[ProtocolVersion]),
      field[PoolingOptions]("poolingOptions", _.withPoolingOptions)(poolingOptionsRead),
      field[QueryOptions]("queryOptions", _.withQueryOptions)(queryOptionsRead),
      field[SocketOptions]("socketOptions", _.withSocketOptions)(socketOptionsRead)
    )
  }

  implicit def clusterBuilderDecoder(
      implicit R1: Read[Config, String],
      R2: Read[Config, Int],
      R3: Read[Config, Boolean],
      R4: Read[Config, List[String]]): Decoder[Config, Cluster.Builder] =
    new ClusterDecoderBuilder().build

}
