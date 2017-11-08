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
  import freestyle.async.implicits._
  import freestyle.cassandra.handlers.implicits._

  val handler: ResultSetAPIHandler[Future] = resultSetAPIHandler[Future]

  implicit val printer: Printer = identityPrinter

  implicit val protocolVersion: ProtocolVersion   = ProtocolVersion.V3
  implicit val stringTypeCodec: TypeCodec[String] = TypeCodec.ascii()
  implicit val uuidTypeCodec: TypeCodec[UUID]     = TypeCodec.uuid()
  import freestyle.cassandra.codecs._

  val reader: FromReader[User] = implicitly[FromReader[User]]

  "ResultSetAPIHandler.read" should {

    "return the valid response for a valid ResultSet" in {
      check {
        forAll(rowAndDataGen[User]) {
          case (resultSet, Nil) =>
            runKFailed(handler.read[User](reader), resultSet) isLikeTo {
              _.isInstanceOf[IllegalStateException]
            }
          case (resultSet, list) =>
            runK(handler.read[User](reader), resultSet) isEqualTo list.head
        }
      }
    }

    "return a failed future for an invalid ResultSet" in {
      runKFailed(handler.read[User](reader), ResultSetBuilder.error) shouldBe ResultSetBuilder.exception
    }
  }

  "ResultSetAPIHandler.readOption" should {

    "return the valid response for a valid ResultSet" in {
      check {
        forAll(rowAndDataGen[User]) {
          case (resultSet, Nil) =>
            runK(handler.readOption[User](reader), resultSet).isEmpty
          case (resultSet, list) =>
            runK(handler.readOption[User](reader), resultSet) == list.headOption
        }
      }
    }

    "return a failed future for an invalid ResultSet" in {
      runKFailed(handler.readOption[User](reader), ResultSetBuilder.error) shouldBe ResultSetBuilder.exception
    }
  }

  "ResultSetAPIHandler.readList" should {

    "return the valid response for a valid ResultSet" in {
      check {
        forAll(rowAndDataGen[User]) {
          case (resultSet, list) =>
            runK(handler.readList[User](reader), resultSet) == list
        }
      }
    }

    "return a failed future for an invalid ResultSet" in {
      runKFailed(handler.readList[User](reader), ResultSetBuilder.error) shouldBe ResultSetBuilder.exception
    }
  }

}
