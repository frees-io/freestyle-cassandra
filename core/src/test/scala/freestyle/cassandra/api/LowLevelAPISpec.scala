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

package freestyle.cassandra.api

import cats.{~>, Id}
import com.datastax.driver.core._
import freestyle._
import freestyle.cassandra.TestUtils
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}

class LowLevelAPISpec
    extends WordSpec
    with Matchers
    with OneInstancePerTest
    with MockFactory
    with TestUtils {

  val sessionMock: Session      = stub[Session]
  val unit: Unit                = ()
  val prepSt: PreparedStatement = stub[PreparedStatement]
  val resultSet: ResultSet      = stub[ResultSet]

  implicit val lowLevelAPIHandler: LowLevelAPI.Op ~> Id = new (LowLevelAPI.Op ~> Id) {
    override def apply[A](fa: LowLevelAPI.Op[A]): Id[A] = fa match {
      case LowLevelAPI.InitOP()                  => sessionMock
      case LowLevelAPI.CloseOP()                 => unit
      case LowLevelAPI.PrepareOP(_)              => prepSt
      case LowLevelAPI.PrepareStatementOP(_)     => prepSt
      case LowLevelAPI.ExecuteOP(_)              => resultSet
      case LowLevelAPI.ExecuteWithValuesOP(_, _) => resultSet
      case LowLevelAPI.ExecuteWithMapOP(_, _)    => resultSet
      case LowLevelAPI.ExecuteStatementOP(_)     => resultSet
    }
  }

  "LowLevelAPI" should {

    "work as expect when calling OP" in {

      type ReturnResult = (
          Session,
          Unit,
          PreparedStatement,
          PreparedStatement,
          ResultSet,
          ResultSet,
          ResultSet,
          ResultSet)

      def program[F[_]](implicit lowLevelAPI: LowLevelAPI[F]): FreeS[F, ReturnResult] = {
        for {
          v1 <- lowLevelAPI.init
          v2 <- lowLevelAPI.close
          v3 <- lowLevelAPI.prepare("")
          v4 <- lowLevelAPI.prepareStatement(null)
          v5 <- lowLevelAPI.execute("")
          v6 <- lowLevelAPI.executeWithValues("", null)
          v7 <- lowLevelAPI.executeWithMap("", null)
          v8 <- lowLevelAPI.executeStatement(null)
        } yield (v1, v2, v3, v4, v5, v6, v7, v8)
      }

      val result = program[LowLevelAPI.Op].interpret[Id]
      result shouldBe (
        (
          sessionMock,
          unit,
          prepSt,
          prepSt,
          resultSet,
          resultSet,
          resultSet,
          resultSet))
    }

  }

}
