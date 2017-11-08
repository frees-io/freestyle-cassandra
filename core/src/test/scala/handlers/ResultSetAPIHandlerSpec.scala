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
package handlers

import java.util.UUID

import com.datastax.driver.core._
import freestyle.cassandra.TestData._
import freestyle.cassandra.TestUtils._
import freestyle.cassandra.query.FieldLister._
import freestyle.cassandra.query.mapper.FieldListMapper._
import freestyle.cassandra.query._
import freestyle.cassandra.query.QueryArbitraries._
import freestyle.cassandra.query.mapper.FromReader
import freestyle.cassandra.query.mapper.GenericFromReader._
import org.scalacheck.Prop._
import org.scalatest.prop.Checkers
import org.scalatest.{OneInstancePerTest, WordSpec}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class ResultSetAPIHandlerSpec
    extends WordSpec
    with MatchersUtil
    with Checkers
    with OneInstancePerTest {

  import cats.instances.future._

  val handler: ResultSetAPIHandler[Future] = new ResultSetAPIHandler[Future]

  implicit val printer: Printer = identityPrinter

  implicit val protocolVersion: ProtocolVersion   = ProtocolVersion.V3
  implicit val stringTypeCodec: TypeCodec[String] = TypeCodec.ascii()
  implicit val uuidTypeCodec: TypeCodec[UUID]     = TypeCodec.uuid()

  val reader: FromReader[User] = implicitly[FromReader[User]]

  "ResultSetAPIHandler.read" should {

    "return the valid response for a valid ResultSet" in {
      check {
        forAll(rowAndDataGen[User]) {
          case (resultSet, Nil) =>
            runFFailed(handler.read[User](resultSet, reader)) isLikeTo {
              _.isInstanceOf[IllegalStateException]
            }
          case (resultSet, list) =>
            runF(handler.read[User](resultSet, reader)) isEqualTo list.head
        }
      }
    }

    "return a failed future for an invalid ResultSet" in {
      runFFailed(handler.read[User](ResultSetBuilder.error, reader)) shouldBe ResultSetBuilder.exception
    }
  }

  "ResultSetAPIHandler.readOption" should {

    "return the valid response for a valid ResultSet" in {
      check {
        forAll(rowAndDataGen[User]) {
          case (resultSet, Nil) =>
            runF(handler.readOption[User](resultSet, reader)).isEmpty
          case (resultSet, list) =>
            runF(handler.readOption[User](resultSet, reader)) == list.headOption
        }
      }
    }

    "return a failed future for an invalid ResultSet" in {
      runFFailed(handler.readOption[User](ResultSetBuilder.error, reader)) shouldBe ResultSetBuilder.exception
    }
  }

  "ResultSetAPIHandler.readList" should {

    "return the valid response for a valid ResultSet" in {
      check {
        forAll(rowAndDataGen[User]) {
          case (resultSet, list) =>
            runF(handler.readList[User](resultSet, reader)) == list
        }
      }
    }

    "return a failed future for an invalid ResultSet" in {
      runFFailed(handler.readList[User](ResultSetBuilder.error, reader)) shouldBe ResultSetBuilder.exception
    }
  }

}
