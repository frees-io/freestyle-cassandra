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
package config.implicits

import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.Executor

import cats.implicits._
import classy.DecodeError.WrongType
import classy.config._
import classy.{DecodeError, Decoder, Read}
import com.datastax.driver.core.ProtocolOptions.Compression
import com.datastax.driver.core.policies._
import com.datastax.driver.core._
import com.typesafe.config.Config
import config._

object datastax {

  import config.implicits._

  implicit val contactPointListRead: Read[Config, ContactPoints] = {
    def trav[T](
        l: List[String],
        f: (String) => Either[DecodeError, T],
        apply: List[T] => ContactPoints): Either[DecodeError, ContactPoints] =
      l.traverse(f).map(apply)

    def inetAddressParser(s: String): Either[DecodeError, InetAddress] = {
      import cats.syntax.either._
      Either.catchNonFatal(InetAddress.getByName(s)).leftMap(_ => WrongType("X.X.X.X", Some(s)))
    }

    def inetSocketAddressParser(s: String): Either[DecodeError, InetSocketAddress] = {
      val SockedAddress = "([^:]+):([0-9]+)".r
      s match {
        case SockedAddress(host, port) => Right(new InetSocketAddress(host, port.toInt))
        case _                         => Left(WrongType("<hostName>:<port>", Some(s)))
      }
    }

    read[List[String], ContactPoints] { list =>
      trav[InetSocketAddress](list, inetSocketAddressParser, ContactPointWithPortList)
        .recoverWith {
          case _ =>
            trav[InetAddress](list, inetAddressParser, ContactPointList)
        } match {
        case Right(t) => Decoder.const(t)
        case Left(e)  => Decoder.fail(e)
      }
    }
  }

  implicit val executorRead                   = instanceRead[Executor]
  implicit val addressTranslatorRead          = instanceRead[AddressTranslator]
  implicit val authProviderRead               = instanceRead[AuthProvider]
  implicit val loadBalancingRead              = instanceRead[LoadBalancingPolicy]
  implicit val reconnectionPolicyRead         = instanceRead[ReconnectionPolicy]
  implicit val retryPolicyRead                = instanceRead[RetryPolicy]
  implicit val speculativeExecutionPolicyRead = instanceRead[SpeculativeExecutionPolicy]
  implicit val sslOptionsRead                 = instanceRead[SSLOptions]
  implicit val threadingOptionsRead           = instanceRead[ThreadingOptions]
  implicit val timestampGeneratorRead         = instanceRead[TimestampGenerator]

  implicit val hostDistances: List[(String, HostDistance)] =
    ("ignored", HostDistance.IGNORED) ::
      ("local", HostDistance.LOCAL) ::
      ("remote", HostDistance.REMOTE) :: Nil

  implicit val consistencyLevels: List[(String, ConsistencyLevel)] =
    ("ALL", ConsistencyLevel.ALL) ::
      ("ANY", ConsistencyLevel.ANY) ::
      ("EACH_QUORUM", ConsistencyLevel.EACH_QUORUM) ::
      ("LOCAL_ONE", ConsistencyLevel.LOCAL_ONE) ::
      ("LOCAL_QUORUM", ConsistencyLevel.LOCAL_QUORUM) ::
      ("LOCAL_SERIAL", ConsistencyLevel.LOCAL_SERIAL) ::
      ("ONE", ConsistencyLevel.ONE) ::
      ("QUORUM", ConsistencyLevel.QUORUM) ::
      ("SERIAL", ConsistencyLevel.SERIAL) ::
      ("THREE", ConsistencyLevel.THREE) ::
      ("TWO", ConsistencyLevel.TWO) :: Nil

  implicit val protocolVersions: List[(String, ProtocolVersion)] =
    ("V1", ProtocolVersion.V1) ::
      ("V2", ProtocolVersion.V2) ::
      ("V3", ProtocolVersion.V3) ::
      ("V4", ProtocolVersion.V4) ::
      ("V5", ProtocolVersion.V5) :: Nil

  implicit val compressions: List[(String, Compression)] =
    ("lz4", Compression.LZ4) :: ("snappy", Compression.SNAPPY) :: ("none", Compression.NONE) :: Nil

  implicit val credentialsRead: Read[Config, Credentials] = Read.instance[Config, Credentials] {
    path =>
      readConfig[String](s"$path.username")
        .join(readConfig[String](s"$path.password"))
        .map(Credentials.tupled)
  }

  implicit val connectionsPerHostDecoder: Read[Config, ConnectionsPerHost] =
    Read.instance[Config, ConnectionsPerHost] { path =>
      readConfig[HostDistance](s"$path.instance")
        .join(readConfig[Int](s"$path.core"))
        .join(readConfig[Int](s"$path.max"))
        .map(ConnectionsPerHost.tupled)
    }

  implicit val coreConnectionsPerHostDecoder: Read[Config, CoreConnectionsPerHost] =
    Read.instance[Config, CoreConnectionsPerHost] { path =>
      readConfig[HostDistance](s"$path.distance")
        .join(readConfig[Int](s"$path.newCoreConnections"))
        .map(CoreConnectionsPerHost.tupled)
    }

  implicit val maxConnectionsPerHostDecoder: Read[Config, MaxConnectionsPerHost] =
    Read.instance[Config, MaxConnectionsPerHost] { path =>
      readConfig[HostDistance](s"$path.distance")
        .join(readConfig[Int](s"$path.maxCoreConnections"))
        .map(MaxConnectionsPerHost.tupled)
    }

  implicit val maxRequestsPerHostDecoder: Read[Config, MaxRequestsPerConnection] =
    Read.instance[Config, MaxRequestsPerConnection] { path =>
      readConfig[HostDistance](s"$path.distance")
        .join(readConfig[Int](s"$path.newMaxRequests"))
        .map(MaxRequestsPerConnection.tupled)
    }

  implicit val newConnectionThresholdDecoder: Read[Config, NewConnectionThreshold] =
    Read.instance[Config, NewConnectionThreshold] { path =>
      readConfig[HostDistance](s"$path.distance")
        .join(readConfig[Int](s"$path.newValue"))
        .map(NewConnectionThreshold.tupled)
    }

}
