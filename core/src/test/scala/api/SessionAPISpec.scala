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
package api

import cats.{~>, Id}
import com.datastax.driver.core._
import freestyle.free._
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}

class SessionAPISpec extends WordSpec with Matchers with OneInstancePerTest with MockFactory {

  import TestUtils._

  val sessionMock: Session      = stub[Session]
  val unit: Unit                = ()
  val prepSt: PreparedStatement = stub[PreparedStatement]
  val resultSet: ResultSet      = stub[ResultSet]

  implicit val sessionAPIHandler: SessionAPI.Op ~> Id = new (SessionAPI.Op ~> Id) {
    override def apply[A](fa: SessionAPI.Op[A]): Id[A] = fa match {
      case SessionAPI.InitOp()                         => sessionMock
      case SessionAPI.CloseOp()                        => unit
      case SessionAPI.PrepareOp(_)                     => prepSt
      case SessionAPI.PrepareStatementOp(_)            => prepSt
      case SessionAPI.ExecuteOp(_)                     => resultSet
      case SessionAPI.ExecuteWithValuesOp(_, _)        => resultSet
      case SessionAPI.ExecuteWithMapOp(_, _)           => resultSet
      case SessionAPI.ExecuteStatementOp(_)            => resultSet
      case SessionAPI.ExecuteWithByteBufferOp(_, _, _) => resultSet
    }
  }

  "SessionAPI" should {

    "work as expect when calling OP" in {

      type ReturnResult = (
          Session,
          Unit,
          PreparedStatement,
          PreparedStatement,
          ResultSet,
          ResultSet,
          ResultSet,
          ResultSet,
          ResultSet)

      def program[F[_]](implicit sessionAPI: SessionAPI[F]): FreeS[F, ReturnResult] = {
        for {
          v1 <- sessionAPI.init
          v2 <- sessionAPI.close
          v3 <- sessionAPI.prepare("")
          v4 <- sessionAPI.prepareStatement(Null[RegularStatement])
          v5 <- sessionAPI.execute("")
          v6 <- sessionAPI.executeWithValues("", Null[Any])
          v7 <- sessionAPI.executeWithMap("", Null[Map[String, AnyRef]])
          v8 <- sessionAPI.executeStatement(Null[Statement])
          v9 <- sessionAPI.executeWithByteBuffer("", Nil, None)
        } yield (v1, v2, v3, v4, v5, v6, v7, v8, v9)
      }

      val result = program[SessionAPI.Op].interpret[Id]
      result shouldBe (
        (
          sessionMock,
          unit,
          prepSt,
          prepSt,
          resultSet,
          resultSet,
          resultSet,
          resultSet,
          resultSet))
    }

  }

}
