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
package api

import cats.{~>, Id}
import com.datastax.driver.core._
import freestyle._
import freestyle.cassandra.query.mapper.GenericFromReader._
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}

class ResultSetAPISpec extends WordSpec with Matchers with OneInstancePerTest with MockFactory {

  val sessionMock: Session      = stub[Session]
  val unit: Unit                = ()
  val prepSt: PreparedStatement = stub[PreparedStatement]
  val resultSet: ResultSet      = stub[ResultSet]

  case class Test()

  val test: Test = Test()

  implicit val resultSetAPIHandler: ResultSetAPI.Op ~> Id = new (ResultSetAPI.Op ~> Id) {
    override def apply[A](fa: ResultSetAPI.Op[A]): Id[A] = fa match {
      case ResultSetAPI.ReadOp(_)       => test.asInstanceOf[A]
      case ResultSetAPI.ReadOptionOp(_) => Some(test)
      case ResultSetAPI.ReadListOp(_)   => List(test)
    }
  }

  "ResultSetAPI" should {

    "work as expect when calling OP" in {

      type ReturnResult = (Test, Option[Test], List[Test])

      def program[F[_]](implicit api: ResultSetAPI[F]): FreeS[F, ReturnResult] = {
        for {
          v1 <- api.read[Test]
          v2 <- api.readOption[Test]
          v3 <- api.readList[Test]
        } yield (v1, v2, v3)
      }

      val result = program[ResultSetAPI.Op].interpret[Id]
      result shouldBe ((test, Some(test), List(test)))
    }

  }

}
