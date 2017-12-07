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

import classy.DecodeError.WrongType
import com.typesafe.config.ConfigFactory
import org.scalacheck.Prop._
import org.scalatest.prop.Checkers
import org.scalatest.{Matchers, WordSpec}

class DatastaxReadsSpec extends WordSpec with Matchers with Checkers {

  import classy.config._
  import com.typesafe.config.Config
  import freestyle.cassandra.config.ConfigArbitraries._

  val validDatastaxReads: DatastaxReads[Config] = new DatastaxReads[Config] {}

  val exception: RuntimeException = new RuntimeException("Test message")

  val invalidDatastaxReads: DatastaxReads[Config] = new DatastaxReads[Config] {
    override def inetAddress(address: String): InetAddress = throw exception

    override def inetSocketAddress(host: String, port: Int): InetSocketAddress = throw exception
  }

  "DatastaxReads inetAddressParser" should {

    val wrongType = WrongType("X.X.X.X", Option(""))

    "return a Right if the inet builder return a InetAddress" in {
      validDatastaxReads.inetAddressParser("10.10.10.10").isRight shouldBe true
    }

    "return a Left if the inet builder throws an exception" in {
      invalidDatastaxReads.inetAddressParser("") shouldBe Left(wrongType)
    }

  }

  "DatastaxReads inetSocketAddressParser" should {

    val wrongType = WrongType("<hostName>:<port>", Option("10.10.10.10"))

    "return a Right if the inet builder return a InetSocketAddress" in {
      validDatastaxReads.inetSocketAddressParser("10.10.10.10:8080").isRight shouldBe true
    }

    "return a Left if the string is not a valid InetSocketAddress" in {
      validDatastaxReads.inetSocketAddressParser("10.10.10.10") shouldBe Left(wrongType)
    }

    "return a Left if the inet builder throws an exception" in {
      invalidDatastaxReads.inetSocketAddressParser("10.10.10.10") shouldBe Left(wrongType)
    }

  }

  "DatastaxReads readOption2" should {

    import validDatastaxReads._

    "work as expected" in {

      check {
        forAll { ov: OptionalValues2 =>
          val decoder = readConfig[Config]("config") andThen readOption2[String, String, String](
            ReadAndPath[String](ov.v1.path),
            ReadAndPath[String](ov.v2.path)) {
            case (s1, s2) => s1 + "-" + s2
          }

          val rawConfig = ConfigFactory.parseString(ov.config)
          val result    = decoder(rawConfig)

          (ov.v1.value, ov.v2.value) match {
            case (Some(v1), Some(v2)) => result == Right(Some(s"$v1-$v2"))
            case (None, None)         => result == Right(None)
            case _                    => result.isLeft
          }
        }
      }

    }

  }

  "DatastaxReads readOption3" should {

    import validDatastaxReads._

    "work as expected" in {

      check {
        forAll { ov: OptionalValues3 =>
          val decoder = readConfig[Config]("config") andThen readOption3[
            String,
            String,
            String,
            String](
            ReadAndPath[String](ov.v1.path),
            ReadAndPath[String](ov.v2.path),
            ReadAndPath[String](ov.v3.path)) {
            case (s1, s2, s3) => s1 + "-" + s2 + "-" + s3
          }

          val rawConfig = ConfigFactory.parseString(ov.config)
          val result    = decoder(rawConfig)

          (ov.v1.value, ov.v2.value, ov.v3.value) match {
            case (Some(v1), Some(v2), Some(v3)) => result == Right(Some(s"$v1-$v2-$v3"))
            case (None, None, None)             => result == Right(None)
            case _                              => result.isLeft
          }
        }
      }

    }

  }

}
