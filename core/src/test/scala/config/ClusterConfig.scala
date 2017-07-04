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

import com.datastax.driver.core.ProtocolOptions.Compression
import com.datastax.driver.core.{ConsistencyLevel, HostDistance, ProtocolVersion}

object ClusterConfig {

  import freestyle.cassandra.config.reads.maps._

  sealed trait ConfigObj extends Product with Serializable {
    def print: String
  }

  case class IpConfig(n1: Int, n2: Int, n3: Int, n4: Int) extends ConfigObj {
    override def print: String = s""" "$n1.$n2.$n3.$n4" """
  }

  case class CredentialsConfig(username: String, password: String) extends ConfigObj {
    override def print: String =
      s"""{
         |   username = "$username"
         |   password = "$password"
         | }""".stripMargin
  }

  case class CompressionConfig(compression: Compression) extends ConfigObj {
    override def print: String = printEnum(compressions, compression)
  }

  case class ProtocolVersionConfig(protocol: ProtocolVersion) extends ConfigObj {
    override def print: String = printEnum(protocolVersions, protocol)
  }

  case class ConsistencyLevelConfig(consistency: ConsistencyLevel) extends ConfigObj {
    override def print: String = printEnum(consistencyLevels, consistency)
  }

  case class ConnectionsPerHost(distance: HostDistance, code: Int, max: Int) extends ConfigObj {
    override def print: String =
      s"""{
         |  distance = ${printEnum(hostDistances, distance)}
         |  code = $code
         |  max = $max
         |}""".stripMargin
  }

  case class CoreConnectionsPerHost(distance: HostDistance, newCoreConnections: Int)
      extends ConfigObj {
    override def print: String =
      s"""{
         |  distance = ${printEnum(hostDistances, distance)}
         |  newCoreConnections = $newCoreConnections
         |}""".stripMargin
  }

  case class MaxConnectionsPerHost(distance: HostDistance, maxCoreConnections: Int)
      extends ConfigObj {
    override def print: String =
      s"""{
         |  distance = ${printEnum(hostDistances, distance)}
         |  maxCoreConnections = $maxCoreConnections
         |}""".stripMargin
  }

  case class MaxRequestsPerConnection(distance: HostDistance, newMaxRequests: Int)
      extends ConfigObj {
    override def print: String =
      s"""{
         |  distance = ${printEnum(hostDistances, distance)}
         |  newMaxRequests = $newMaxRequests
         |}""".stripMargin
  }

  case class NewConnectionThreshold(distance: HostDistance, newValue: Int) extends ConfigObj {
    override def print: String =
      s"""{
         |  distance = ${printEnum(hostDistances, distance)}
         |  newValue = $newValue
         |}""".stripMargin
  }

  case class PoolingOptionsConfig(
      connectionsPerHost: Option[ConnectionsPerHost] = None,
      coreConnectionsPerHost: Option[CoreConnectionsPerHost] = None,
      maxConnectionsPerHost: Option[MaxConnectionsPerHost] = None,
      maxRequestsPerConnection: Option[MaxRequestsPerConnection] = None,
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
        "maxRequestsPerConnection" -> maxRequestsPerConnection,
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

  private[this] def printOpt[T](t: (String, Option[T])): Option[String] = t match {
    case (l, Some(s: String))    => Some(s""" $l = "$s" """)
    case (l, Some(o: ConfigObj)) => Some(s""" $l = ${o.print} """)
    case (l, Some(a))            => Some(s""" $l = ${a.toString} """)
    case (_, None)               => None
  }

  private[this] def printEnum[T](map: Map[String, T], value: T): String =
    map.find(_._2 == value).map(t => s""" "${t._1}" """).getOrElse("")

}
