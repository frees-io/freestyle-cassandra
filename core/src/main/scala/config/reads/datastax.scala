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
package config.reads

import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.Executor

import cats.implicits._
import classy.DecodeError.{Underlying, WrongType}
import classy.{DecodeError, Decoder, Read}
import com.datastax.driver.core.ProtocolOptions.Compression
import com.datastax.driver.core._
import com.datastax.driver.core.policies._
import freestyle.cassandra.config.model._

import scala.util.{Failure, Success, Try}

trait DatastaxReads[Config] {

  import maps._

  def instanceRead[T](implicit R: Read[Config, String]): Read[Config, T] =
    read[String, T] { value =>
      Try(Class.forName(value).newInstance().asInstanceOf[T]) match {
        case Failure(e) => Decoder.fail(Underlying(e))
        case Success(c) => Decoder.const(c)
      }
    }

  def stringListRead[T](implicit map: Map[String, T], R: Read[Config, String]): Read[Config, T] =
    read[String, T] { value =>
      map.get(value) match {
        case Some(v) => Decoder.const(v)
        case None    => Decoder.fail(WrongType(map.keys.mkString(" | "), Some(value)))
      }
    }

  def read[A, B](f: A => Decoder[Config, B])(implicit R: Read[Config, A]): Read[Config, B] =
    Read.instance[Config, B](path => R.apply(path).flatMap(f))

  def contactPointListRead(implicit R: Read[Config, List[String]]): Read[Config, ContactPoints] = {

    def trav[T](
        l: List[String],
        f: (String) => Either[DecodeError, T],
        apply: List[T] => ContactPoints): Either[DecodeError, ContactPoints] =
      l.traverse(f).map(apply)

    def inetAddressParser(s: String): Either[DecodeError, InetAddress] =
      Either.catchNonFatal(InetAddress.getByName(s)).leftMap(_ => WrongType("X.X.X.X", Some(s)))

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

  def javaExecutorRead(implicit R: Read[Config, String]): Read[Config, Executor] =
    instanceRead[Executor]
  def addressTranslatorRead(implicit R: Read[Config, String]): Read[Config, AddressTranslator] =
    instanceRead[AddressTranslator]
  def authProviderRead(implicit R: Read[Config, String]): Read[Config, AuthProvider] =
    instanceRead[AuthProvider]
  def loadBalancingRead(implicit R: Read[Config, String]): Read[Config, LoadBalancingPolicy] =
    instanceRead[LoadBalancingPolicy]
  def reconnectionPolicyRead(implicit R: Read[Config, String]): Read[Config, ReconnectionPolicy] =
    instanceRead[ReconnectionPolicy]
  def retryPolicyRead(implicit R: Read[Config, String]): Read[Config, RetryPolicy] =
    instanceRead[RetryPolicy]
  def executionPolicyRead(
      implicit R: Read[Config, String]): Read[Config, SpeculativeExecutionPolicy] =
    instanceRead[SpeculativeExecutionPolicy]
  def sslOptionsRead(implicit R: Read[Config, String]): Read[Config, SSLOptions] =
    instanceRead[SSLOptions]
  def threadingOptionsRead(implicit R: Read[Config, String]): Read[Config, ThreadingOptions] =
    instanceRead[ThreadingOptions]
  def timestampGeneratorRead(implicit R: Read[Config, String]): Read[Config, TimestampGenerator] =
    instanceRead[TimestampGenerator]

  def credentialsDecoder(implicit R: Read[Config, String]): Read[Config, Credentials] =
    Read.instance[Config, Credentials] { path =>
      Read
        .from[Config][String](s"$path.username")
        .join(Read.from[Config][String](s"$path.password"))
        .map(Credentials.tupled)
    }

  def connectionsPerHostDecoder(
      implicit R1: Read[Config, String],
      R2: Read[Config, Int]): Read[Config, ConnectionsPerHost] =
    Read.instance[Config, ConnectionsPerHost] { path =>
      Read
        .from[Config][HostDistance](s"$path.distance")(stringListRead[HostDistance])
        .join(Read.from[Config][Int](s"$path.core"))
        .join(Read.from[Config][Int](s"$path.max"))
        .map(ConnectionsPerHost.tupled)
    }

  def coreConnectionsPerHostDecoder(
      implicit R1: Read[Config, String],
      R2: Read[Config, Int]): Read[Config, CoreConnectionsPerHost] =
    Read.instance[Config, CoreConnectionsPerHost] { path =>
      Read
        .from[Config][HostDistance](s"$path.distance")(stringListRead[HostDistance])
        .join(Read.from[Config][Int](s"$path.newCoreConnections"))
        .map(CoreConnectionsPerHost.tupled)
    }

  def maxConnectionsPerHostDecoder(
      implicit R1: Read[Config, String],
      R2: Read[Config, Int]): Read[Config, MaxConnectionsPerHost] =
    Read.instance[Config, MaxConnectionsPerHost] { path =>
      Read
        .from[Config][HostDistance](s"$path.distance")(stringListRead[HostDistance])
        .join(Read.from[Config][Int](s"$path.maxCoreConnections"))
        .map(MaxConnectionsPerHost.tupled)
    }

  def maxRequestsPerHostDecoder(
      implicit R1: Read[Config, String],
      R2: Read[Config, Int]): Read[Config, MaxRequestsPerConnection] =
    Read.instance[Config, MaxRequestsPerConnection] { path =>
      Read
        .from[Config][HostDistance](s"$path.distance")(stringListRead[HostDistance])
        .join(Read.from[Config][Int](s"$path.newMaxRequests"))
        .map(MaxRequestsPerConnection.tupled)
    }

  def newConnectionThresholdDecoder(
      implicit R1: Read[Config, String],
      R2: Read[Config, Int]): Read[Config, NewConnectionThreshold] =
    Read.instance[Config, NewConnectionThreshold] { path =>
      Read
        .from[Config][HostDistance](s"$path.distance")(stringListRead[HostDistance])
        .join(Read.from[Config][Int](s"$path.newValue"))
        .map(NewConnectionThreshold.tupled)
    }

}

object maps {

  implicit val hostDistances: Map[String, HostDistance] =
    Map(
      "ignored" -> HostDistance.IGNORED,
      "local"   -> HostDistance.LOCAL,
      "remote"  -> HostDistance.REMOTE)

  implicit val consistencyLevels: Map[String, ConsistencyLevel] =
    Map(
      "ALL"          -> ConsistencyLevel.ALL,
      "ANY"          -> ConsistencyLevel.ANY,
      "EACH_QUORUM"  -> ConsistencyLevel.EACH_QUORUM,
      "LOCAL_ONE"    -> ConsistencyLevel.LOCAL_ONE,
      "LOCAL_QUORUM" -> ConsistencyLevel.LOCAL_QUORUM,
      "LOCAL_SERIAL" -> ConsistencyLevel.LOCAL_SERIAL,
      "ONE"          -> ConsistencyLevel.ONE,
      "QUORUM"       -> ConsistencyLevel.QUORUM,
      "SERIAL"       -> ConsistencyLevel.SERIAL,
      "THREE"        -> ConsistencyLevel.THREE,
      "TWO"          -> ConsistencyLevel.TWO
    )

  implicit val protocolVersions: Map[String, ProtocolVersion] =
    Map(
      "V1" -> ProtocolVersion.V1,
      "V2" -> ProtocolVersion.V2,
      "V3" -> ProtocolVersion.V3,
      "V4" -> ProtocolVersion.V4,
      "V5" -> ProtocolVersion.V5)

  implicit val compressions: Map[String, Compression] =
    Map("lz4" -> Compression.LZ4, "snappy" -> Compression.SNAPPY, "none" -> Compression.NONE)

}
