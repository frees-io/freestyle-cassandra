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

import com.datastax.driver.core.{ConsistencyLevel, HostDistance, ProtocolVersion}
import com.datastax.driver.core.ProtocolOptions.Compression

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

object configTree extends App {

  import freestyle.cassandra.config.reads.maps._

  case class IpConfig(n1: Int, n2: Int, n3: Int, n4: Int) {
    override def toString: String = s""" "$n1.$n2.$n3.$n4" """
  }

  sealed trait ConfigObj extends Product with Serializable {
    def print: String
  }

  case class CredentialsConfig(username: String, password: String) extends ConfigObj {
    override def print: String =
      s"""{
         |   username = "$username"
         |   password = "$password"
         | }""".stripMargin
  }

  case class CompressionConfig(compression: Compression) extends ConfigObj {
    override def print: String =
      compressions.find(_._2 == compression).map(t => s" = ${t._1}").getOrElse("")
  }

  case class ProtocolVersionConfig(protocol: ProtocolVersion) extends ConfigObj {
    override def print: String =
      protocolVersions.find(_._2 == protocol).map(t => s" = ${t._1}").getOrElse("")
  }

  case class ConsistencyLevelConfig(consistency: ConsistencyLevel) extends ConfigObj {
    override def print: String =
      consistencyLevels.find(_._2 == consistency).map(t => s" = ${t._1}").getOrElse("")
  }

  case class ConnectionsPerHost(distance: HostDistance, code: Int, max: Int)
  case class CoreConnectionsPerHost(distance: HostDistance, newCoreConnections: Int)
  case class MaxConnectionsPerHost(distance: HostDistance, maxCoreConnections: Int)
  case class NewConnectionThreshold(distance: HostDistance, newValue: Int)

  case class PoolingOptionsConfig(
      connectionsPerHost: Option[ConnectionsPerHost] = None,
      coreConnectionsPerHost: Option[CoreConnectionsPerHost] = None,
      maxConnectionsPerHost: Option[MaxConnectionsPerHost] = None,
      newConnectionThreshold: Option[NewConnectionThreshold] = None,
      heartbeatIntervalSeconds: Option[Int] = None,
      idleTimeoutSeconds: Option[Int] = None,
      maxQueueSize: Option[Int] = None,
      poolTimeoutMillis: Option[Int] = None,
      initializationExecutor: Option[String] = None)
      extends ConfigObj {
    override def print: String = {
      val fields = List(
        "connectionsPerHost"       -> connectionsPerHost,
        "coreConnectionsPerHost"   -> coreConnectionsPerHost,
        "maxConnectionsPerHost"    -> maxConnectionsPerHost,
        "newConnectionThreshold"   -> newConnectionThreshold,
        "heartbeatIntervalSeconds" -> heartbeatIntervalSeconds,
        "idleTimeoutSeconds"       -> idleTimeoutSeconds,
        "maxQueueSize"             -> maxQueueSize,
        "poolTimeoutMillis"        -> poolTimeoutMillis,
        "initializationExecutor"   -> initializationExecutor
      ).flatMap(printOpt(_).toList)
      ("{" +: fields :+ "}") mkString "\n"
    }
  }

  case class QueryOptionsConfig(
      consistencyLevel: Option[ConsistencyLevelConfig] = None,
      serialConsistencyLevel: Option[ConsistencyLevelConfig] = None,
      defaultIdempotence: Option[Boolean] = None,
      fetchSize: Option[Int] = None,
      maxPendingRefreshNodeListRequests: Option[Int] = None,
      maxPendingRefreshNodeRequests: Option[Int] = None,
      maxPendingRefreshSchemaRequests: Option[Int] = None,
      metadataEnabled: Option[Boolean] = None,
      prepareOnAllHosts: Option[Boolean] = None,
      refreshNodeIntervalMillis: Option[Int] = None,
      refreshNodeListIntervalMillis: Option[Int] = None,
      refreshSchemaIntervalMillis: Option[Int] = None,
      reprepareOnUp: Option[Boolean] = None
  ) extends ConfigObj {
    override def print: String = {
      val fields = List(
        "consistencyLevel"                  -> consistencyLevel,
        "serialConsistencyLevel"            -> serialConsistencyLevel,
        "defaultIdempotence"                -> defaultIdempotence,
        "fetchSize"                         -> fetchSize,
        "maxPendingRefreshNodeListRequests" -> maxPendingRefreshNodeListRequests,
        "maxPendingRefreshNodeRequests"     -> maxPendingRefreshNodeRequests,
        "maxPendingRefreshSchemaRequests"   -> maxPendingRefreshSchemaRequests,
        "metadataEnabled"                   -> metadataEnabled,
        "prepareOnAllHosts"                 -> prepareOnAllHosts,
        "refreshNodeIntervalMillis"         -> refreshNodeIntervalMillis,
        "refreshNodeListIntervalMillis"     -> refreshNodeListIntervalMillis,
        "refreshSchemaIntervalMillis"       -> refreshSchemaIntervalMillis,
        "reprepareOnUp"                     -> reprepareOnUp
      ).flatMap(printOpt(_).toList)
      ("{" +: fields :+ "}") mkString "\n"
    }
  }

  case class SocketOptionsConfig(
      connectTimeoutMillis: Option[Int] = None,
      keepAlive: Option[Boolean] = None,
      readTimeoutMillis: Option[Int] = None,
      receiveBufferSize: Option[Int] = None,
      sendBufferSize: Option[Int] = None,
      reuseAddress: Option[Boolean] = None,
      soLinger: Option[Int] = None
  ) extends ConfigObj {
    override def print: String = {
      val fields = List(
        "connectTimeoutMillis" -> connectTimeoutMillis,
        "keepAlive"            -> keepAlive,
        "readTimeoutMillis"    -> readTimeoutMillis,
        "receiveBufferSize"    -> receiveBufferSize,
        "sendBufferSize"       -> sendBufferSize,
        "reuseAddress"         -> reuseAddress,
        "soLinger"             -> soLinger
      ).flatMap(printOpt(_).toList)
      ("{" +: fields :+ "}") mkString "\n"
    }
  }

  def printOpt[T](t: (String, Option[T])): Option[String] = t match {
    case (l, Some(s: String))    => Some(s""" $l = "$s" """)
    case (l, Some(o: ConfigObj)) => Some(s""" $l ${o.print} """)
    case (l, Some(a))            => Some(s""" $l = ${a.toString} """)
    case (_, None)               => None
  }

  println(
    ClusterConfig(
      contactPoints = List(IpConfig(127, 0, 0, 1)),
      credentials = Some(CredentialsConfig("user", "pass")),
      name = Some("MyCluster"),
      allowBetaProtocolVersion = Some(false)
    ).print)

  case class ClusterConfig(
      contactPoints: List[IpConfig],
      credentials: Option[CredentialsConfig] = None,
      name: Option[String] = None,
      allowBetaProtocolVersion: Option[Boolean] = None,
      enableSSL: Option[Boolean] = None,
      addressTranslator: Option[String] = None,
      authProvider: Option[String] = None,
      loadBalancingPolicy: Option[String] = None,
      reconnectionPolicy: Option[String] = None,
      retryPolicy: Option[String] = None,
      speculativeExecutionPolicy: Option[String] = None,
      sslOptions: Option[String] = None,
      threadingOptions: Option[String] = None,
      timestampGenerator: Option[String] = None,
      maxSchemaAgreementWaitSeconds: Option[Int] = None,
      port: Option[Int] = None,
      compression: Option[CompressionConfig] = None,
      poolingOptionsConfig: Option[PoolingOptionsConfig] = None,
      protocolVersion: Option[CompressionConfig] = None,
      queryOptions: Option[QueryOptionsConfig] = None,
      socketOptions: Option[SocketOptionsConfig] = None)
      extends ConfigObj {
    override def print: String = {
      val fields = s""" contactPoints = [ ${contactPoints.mkString(",")} ] """ +:
        List(
        "credentials"                   -> credentials,
        "name"                          -> name,
        "allowBetaProtocolVersion"      -> allowBetaProtocolVersion,
        "enableSSL"                     -> enableSSL,
        "addressTranslator"             -> addressTranslator,
        "authProvider"                  -> authProvider,
        "loadBalancingPolicy"           -> loadBalancingPolicy,
        "reconnectionPolicy"            -> reconnectionPolicy,
        "retryPolicy"                   -> retryPolicy,
        "speculativeExecutionPolicy"    -> speculativeExecutionPolicy,
        "sslOptions"                    -> sslOptions,
        "threadingOptions"              -> threadingOptions,
        "timestampGenerator"            -> timestampGenerator,
        "maxSchemaAgreementWaitSeconds" -> maxSchemaAgreementWaitSeconds,
        "port"                          -> port,
        "compression"                   -> compression,
        "poolingOptionsConfig"          -> poolingOptionsConfig,
        "protocolVersion"               -> protocolVersion,
        "queryOptions"                  -> queryOptions,
        "socketOptions"                 -> socketOptions
      ).flatMap(printOpt(_).toList)
      ("{" +: fields :+ "}") mkString "\n"
    }
  }

}
