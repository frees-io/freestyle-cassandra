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
import freestyle.cassandra.codecs._
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}

class StatementAPISpec extends WordSpec with Matchers with OneInstancePerTest with MockFactory {

  val prepSt: PreparedStatement = stub[PreparedStatement]
  (prepSt.getVariables _).when().returns(ColumnDefinitionsTest)
  (prepSt.getPreparedId _).when().returns(PreparedIdTest)
  val boundSt1: BoundStatement = new BoundStatement(prepSt)
  val boundSt2: BoundStatement = new BoundStatement(prepSt)
  val boundSt3: BoundStatement = new BoundStatement(prepSt)
  val boundSt4: BoundStatement = new BoundStatement(prepSt)
  val boundSt5: BoundStatement = new BoundStatement(prepSt)
  val boundSt6: BoundStatement = new BoundStatement(prepSt)
  val boundSt7: BoundStatement = new BoundStatement(prepSt)
  val byteBuffer: ByteBuffer   = ByteBuffer.wrap("Hello World!".getBytes)

  implicit val statementAPIHandler: StatementAPI.Op ~> Id = new (StatementAPI.Op ~> Id) {
    override def apply[A](fa: StatementAPI.Op[A]): Id[A] = fa match {
      case StatementAPI.BindOP(_)                        => boundSt1
      case StatementAPI.SetByteBufferByIndexOP(_, _, _)  => boundSt2
      case StatementAPI.SetByteBufferByNameOP(_, _, _)   => boundSt3
      case StatementAPI.SetValueByIndexOP(_, _, _, _)    => boundSt4
      case StatementAPI.SetValueByNameOP(_, _, _, _)     => boundSt5
      case StatementAPI.SetByteBufferListByIndexOP(_, _) => boundSt6
      case StatementAPI.SetByteBufferListByNameOP(_, _)  => boundSt7
    }
  }

  "SessionAPI" should {

    "work as expect when calling OP" in {

      type ReturnResult =
        (
            BoundStatement,
            BoundStatement,
            BoundStatement,
            BoundStatement,
            BoundStatement,
            BoundStatement,
            BoundStatement)

      def program[F[_]](implicit API: StatementAPI[F]): FreeS[F, ReturnResult] = {
        for {
          v1 <- API.bind(prepSt)
          v2 <- API.setByteBufferByIndex(v1, 0, byteBuffer)
          v3 <- API.setByteBufferByName(v2, "", byteBuffer)
          v4 <- API.setValueByIndex[Double](v3, 0, 15.5, doubleCodec)
          v5 <- API.setValueByName[Double](v4, "", 15.5, doubleCodec)
          v6 <- API.setByteBufferListByIndex(prepSt, Nil)
          v7 <- API.setByteBufferListByName(prepSt, Nil)
        } yield (v1, v2, v3, v4, v5, v6, v7)
      }

      val result = program[StatementAPI.Op].interpret[Id]
      result shouldBe ((boundSt1, boundSt2, boundSt3, boundSt4, boundSt5, boundSt6, boundSt7))
    }

  }

}
