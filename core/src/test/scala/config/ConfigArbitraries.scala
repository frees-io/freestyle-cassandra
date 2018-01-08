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
package config

import java.nio.ByteBuffer

import com.datastax.driver.core.ProtocolOptions.Compression
import config.model._
import com.datastax.driver.core._
import com.datastax.driver.core.policies.{
  DefaultRetryPolicy,
  DowngradingConsistencyRetryPolicy,
  FallthroughRetryPolicy,
  RetryPolicy
}
import org.scalacheck.{Arbitrary, Gen}

trait ConfigArbitraries {

  implicit val consistencyLevelArb: Arbitrary[ConsistencyLevel] = Arbitrary {
    Gen.oneOf(
      ConsistencyLevel.ALL,
      ConsistencyLevel.ANY,
      ConsistencyLevel.EACH_QUORUM,
      ConsistencyLevel.LOCAL_ONE,
      ConsistencyLevel.LOCAL_QUORUM,
      ConsistencyLevel.LOCAL_SERIAL,
      ConsistencyLevel.ONE,
      ConsistencyLevel.QUORUM,
      ConsistencyLevel.SERIAL,
      ConsistencyLevel.THREE,
      ConsistencyLevel.TWO
    )
  }

  implicit val protocolVersionArbitrary: Arbitrary[ProtocolVersion] = Arbitrary {
    Gen.oneOf(
      ProtocolVersion.NEWEST_BETA,
      ProtocolVersion.NEWEST_SUPPORTED,
      ProtocolVersion.V1,
      ProtocolVersion.V2,
      ProtocolVersion.V3,
      ProtocolVersion.V4,
      ProtocolVersion.V5
    )
  }

  implicit val compressionArbitrary: Arbitrary[Compression] = Arbitrary {
    Gen.oneOf(Compression.NONE, Compression.LZ4, Compression.SNAPPY)
  }

  implicit val hostDistanceArbitrary: Arbitrary[HostDistance] = Arbitrary {
    Gen.oneOf(HostDistance.IGNORED, HostDistance.LOCAL, HostDistance.REMOTE)
  }

  implicit val byteBufferArbitrary: Arbitrary[ByteBuffer] = Arbitrary {
    Gen.identifier map (s => ByteBuffer.wrap(s.getBytes))
  }

  implicit val rawPagingStateArbitrary: Arbitrary[RawPagingState] = Arbitrary {
    Gen.identifier map (s => RawPagingState(s.getBytes))
  }

  implicit val codecPagingStateArbitrary: Arbitrary[CodecPagingState] = Arbitrary {

    // Valid PagingState String just to skip the validations
    val validPagingState: String =
      "0018001010ed3c639da1694885beaa7812eb9202db00f07ffffffd0090a0593939dbd419cd9f9aa16271a49e0004"

    Gen.option(CodecRegistry.DEFAULT_INSTANCE) map { codecRegistry =>
      CodecPagingState(PagingState.fromString(validPagingState), codecRegistry)
    }
  }

  implicit val retryPolicyArbitrary: Arbitrary[RetryPolicy] = Arbitrary {
    Gen.oneOf(
      DefaultRetryPolicy.INSTANCE,
      DowngradingConsistencyRetryPolicy.INSTANCE,
      FallthroughRetryPolicy.INSTANCE
    )
  }

  case class PathValue(path: String, value: Option[String]) {
    def print: String = value.map(s => s"""$path = "$s" """).getOrElse("")
  }

  case class OptionalValues2(v1: PathValue, v2: PathValue, config: String)

  case class OptionalValues3(v1: PathValue, v2: PathValue, v3: PathValue, config: String)

  def pathValueGen(pos: Int): Gen[PathValue] =
    Gen.option(Gen.identifier).map(v => PathValue(s"path$pos", v))

  implicit val optionalValues2Arbitrary: Arbitrary[OptionalValues2] = Arbitrary {
    for {
      v1 <- pathValueGen(1)
      v2 <- pathValueGen(2)
    } yield
      OptionalValues2(
        v1,
        v2,
        s"""
           |config = {
           |  ${v1.print}
           |  ${v2.print}
           |}""".stripMargin
      )
  }

  implicit val optionalValues3Arbitrary: Arbitrary[OptionalValues3] = Arbitrary {
    for {
      v1 <- pathValueGen(1)
      v2 <- pathValueGen(2)
      v3 <- pathValueGen(3)
    } yield
      OptionalValues3(
        v1,
        v2,
        v3,
        s"""
           |config = {
           |  ${v1.print}
           |  ${v2.print}
           |  ${v3.print}
           |}""".stripMargin
      )
  }

}

object ConfigArbitraries extends ConfigArbitraries
