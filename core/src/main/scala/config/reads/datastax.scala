/*
 * Copyright 2017-2018 47 Degrees, LLC. <http://www.47deg.com>
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
import classy.DecodeError.{AtPath, Missing, Underlying, WrongType}
import classy.{DecodeError, Decoder, Read}
import com.datastax.driver.core._
import com.datastax.driver.core.ProtocolOptions.Compression
import com.datastax.driver.core.policies._
import freestyle.cassandra.config.model._

import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

trait DatastaxReads[Config] extends InetBuilderWrapper {

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

  def inetAddressParser(s: String): Either[DecodeError, InetAddress] =
    Either.catchNonFatal(inetAddress(s)).leftMap(_ => WrongType("X.X.X.X", Some(s)))

  def inetSocketAddressParser(s: String): Either[DecodeError, InetSocketAddress] = {
    val SockedAddress: Regex             = "([^:]+):([0-9]+)".r
    val wrongType: (String) => WrongType = s => WrongType("<hostName>:<port>", Option(s))
    s match {
      case SockedAddress(host, port) =>
        Either
          .catchNonFatal(inetSocketAddress(host, java.lang.Integer.parseInt(port)))
          .leftMap(_ => wrongType(s))
      case _ => Left(wrongType(s))
    }
  }

  def contactPointListRead(
      implicit R: Read[Config, List[String]]): Read[Config, Option[ContactPoints]] = {

    def trav[T](
        l: List[String],
        f: (String) => Either[DecodeError, T],
        apply: List[T] => ContactPoints): Either[DecodeError, ContactPoints] =
      l.traverse(f).map(apply)

    read[List[String], Option[ContactPoints]] { list =>
      trav[InetSocketAddress](list, inetSocketAddressParser, ContactPointWithPortList)
        .recoverWith {
          case _ =>
            trav[InetAddress](list, inetAddressParser, ContactPointList)
        } match {
        case Right(t) => Decoder.const(Some(t))
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

  case class ReadAndPath[T](path: String)(implicit R: Read[Config, T]) {
    def decoder: Decoder[Config, T] = Read.from[Config][T](path)
  }

  def readOption2[T, T1, T2](r1: ReadAndPath[T1], r2: ReadAndPath[T2])(
      f: ((T1, T2)) => T): Decoder[Config, Option[T]] =
    r1.decoder.optional.join(r2.decoder.optional).flatMap {
      case (Some(v1), Some(v2)) => Decoder.const(Some(f((v1, v2))))
      case (None, None)         => Decoder.const(None)
      case (None, _)            => Decoder.fail(AtPath(r1.path, Missing))
      case (_, None)            => Decoder.fail(AtPath(r2.path, Missing))
    }

  def readOption3[T, T1, T2, T3](r1: ReadAndPath[T1], r2: ReadAndPath[T2], r3: ReadAndPath[T3])(
      f: ((T1, T2, T3)) => T): Decoder[Config, Option[T]] =
    r1.decoder.optional.join(r2.decoder.optional).join(r3.decoder.optional).flatMap {
      case (Some(v1), Some(v2), Some(v3)) => Decoder.const(Some(f((v1, v2, v3))))
      case (None, None, None)             => Decoder.const(None)
      case (None, _, _)                   => Decoder.fail(AtPath(r1.path, Missing))
      case (_, None, _)                   => Decoder.fail(AtPath(r2.path, Missing))
      case (_, _, None)                   => Decoder.fail(AtPath(r3.path, Missing))
    }

  def credentialsDecoder(implicit R: Read[Config, String]): Read[Config, Option[Credentials]] =
    Read.instance[Config, Option[Credentials]] { path =>
      readOption2(ReadAndPath[String](s"$path.username"), ReadAndPath[String](s"$path.password"))(
        Credentials.tupled)
    }

  def connectionsPerHostDecoder(
      implicit R1: Read[Config, String],
      R2: Read[Config, Int]): Read[Config, Option[ConnectionsPerHost]] =
    Read.instance[Config, Option[ConnectionsPerHost]] { path =>
      readOption3(
        ReadAndPath[HostDistance](s"$path.distance")(stringListRead[HostDistance]),
        ReadAndPath[Int](s"$path.core"),
        ReadAndPath[Int](s"$path.max"))(ConnectionsPerHost.tupled)
    }

  def coreConnectionsPerHostDecoder(
      implicit R1: Read[Config, String],
      R2: Read[Config, Int]): Read[Config, Option[CoreConnectionsPerHost]] =
    Read.instance[Config, Option[CoreConnectionsPerHost]] { path =>
      readOption2(
        ReadAndPath[HostDistance](s"$path.distance")(stringListRead[HostDistance]),
        ReadAndPath[Int](s"$path.newCoreConnections"))(CoreConnectionsPerHost.tupled)
    }

  def maxConnectionsPerHostDecoder(
      implicit R1: Read[Config, String],
      R2: Read[Config, Int]): Read[Config, Option[MaxConnectionsPerHost]] =
    Read.instance[Config, Option[MaxConnectionsPerHost]] { path =>
      readOption2(
        ReadAndPath[HostDistance](s"$path.distance")(stringListRead[HostDistance]),
        ReadAndPath[Int](s"$path.maxCoreConnections"))(MaxConnectionsPerHost.tupled)
    }

  def maxRequestsPerConnectionDecoder(
      implicit R1: Read[Config, String],
      R2: Read[Config, Int]): Read[Config, Option[MaxRequestsPerConnection]] =
    Read.instance[Config, Option[MaxRequestsPerConnection]] { path =>
      readOption2(
        ReadAndPath[HostDistance](s"$path.distance")(stringListRead[HostDistance]),
        ReadAndPath[Int](s"$path.newMaxRequests"))(MaxRequestsPerConnection.tupled)
    }

  def newConnectionThresholdDecoder(
      implicit R1: Read[Config, String],
      R2: Read[Config, Int]): Read[Config, Option[NewConnectionThreshold]] =
    Read.instance[Config, Option[NewConnectionThreshold]] { path =>
      readOption2(
        ReadAndPath[HostDistance](s"$path.distance")(stringListRead[HostDistance]),
        ReadAndPath[Int](s"$path.newValue"))(NewConnectionThreshold.tupled)
    }

}

trait InetBuilderWrapper {

  def inetAddress(address: String): InetAddress = InetAddress.getByName(address)

  def inetSocketAddress(host: String, port: Int): InetSocketAddress =
    new InetSocketAddress(host, port)

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
