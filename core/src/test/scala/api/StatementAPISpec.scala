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

import java.nio.ByteBuffer

import cats.{~>, Id}
import com.datastax.driver.core._
import freestyle._
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}

class StatementAPISpec extends WordSpec with Matchers with OneInstancePerTest with MockFactory {

  val prepSt: PreparedStatement = stub[PreparedStatement]
  (prepSt.getVariables _).when().returns(ColumnDefinitionsTest)
  (prepSt.getPreparedId _).when().returns(PreparedIdTest)
  val boundSt: BoundStatement = new BoundStatement(prepSt)
  val byteBuffer: ByteBuffer  = ByteBuffer.wrap("Hello World!".getBytes)

  implicit val statementAPIHandler: StatementAPI.Op ~> Id = new (StatementAPI.Op ~> Id) {
    override def apply[A](fa: StatementAPI.Op[A]): Id[A] = fa match {
      case StatementAPI.BindOP(_)                      => boundSt
      case StatementAPI.SetBytesUnsafeIndexOP(_, _, _) => boundSt
      case StatementAPI.SetBytesUnsafeNameOP(_, _, _)  => boundSt
    }
  }

  "SessionAPI" should {

    "work as expect when calling OP" in {

      type ReturnResult = (BoundStatement, BoundStatement, BoundStatement)

      def program[F[_]](implicit API: StatementAPI[F]): FreeS[F, ReturnResult] = {
        for {
          v1 <- API.bind(prepSt)
          v2 <- API.setBytesUnsafeIndex(boundSt, 0, byteBuffer)
          v3 <- API.setBytesUnsafeName(boundSt, "", byteBuffer)
        } yield (v1, v2, v3)
      }

      val result = program[StatementAPI.Op].interpret[Id]
      result shouldBe ((boundSt, boundSt, boundSt))
    }

  }

}
