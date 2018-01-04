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
package handlers

import java.nio.ByteBuffer

import cats.MonadError
import com.datastax.driver.core._
import freestyle.cassandra.TestUtils._
import freestyle.cassandra.codecs._
import freestyle.cassandra.query.model.{SerializableValue, SerializableValueBy}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

class StatementAPIHandlerSpec
    extends WordSpec
    with Matchers
    with OneInstancePerTest
    with MockFactory {

  val prepStMock: PreparedStatement = stub[PreparedStatement]
  (prepStMock.getVariables _).when().returns(ColumnDefinitionsTest)
  (prepStMock.getPreparedId _).when().returns(PreparedIdTest)
  (prepStMock.getConsistencyLevel _).when().returns(ConsistencyLevel.ALL)

  val boundedStMock: BoundStatement = new BoundStatement(prepStMock)

  val boundStMock: BoundStatement = new BoundStatement(prepStMock) {
    override def setBytesUnsafe(i: Int, v: ByteBuffer): BoundStatement       = boundedStMock
    override def setBytesUnsafe(name: String, v: ByteBuffer): BoundStatement = boundedStMock
  }

  (prepStMock.bind _).when().returns(boundStMock)

  val byteBuffer: ByteBuffer = ByteBuffer.wrap("Hello World!".getBytes)
  val serializableValue: SerializableValue = new SerializableValue {
    override def serialize[M[_]](implicit E: MonadError[M, Throwable]): M[ByteBuffer] =
      E.pure(byteBuffer)
  }

  import cats.instances.future._
  import freestyle.cassandra.implicits._
  val handler: StatementAPIHandler[Future] = new StatementAPIHandler[Future]

  "StatementAPIHandler" should {

    "call to bind when calling bind(PreparedStatement) method" in {
      runF(handler.bind(prepStMock)) shouldBe boundStMock
      (prepStMock.bind _).verify()
    }

    "call to setBytesUnsafe when calling setByteBufferByIndex(BoundStatement, Int, ByteBuffer) method" in {
      runF(handler.setByteBufferByIndex(boundStMock, 10, byteBuffer)) shouldBe boundedStMock
    }

    "call to setBytesUnsafe when calling setByteBufferByName(BoundStatement, String, ByteBuffer) method" in {
      runF(handler.setByteBufferByName(boundStMock, "name", byteBuffer)) shouldBe boundedStMock
    }

    "call to setBytesUnsafe when calling setValueByIndex[T](BoundStatement, Int, T, ByteBufferCodec[T]) method" in {
      runF(handler.setValueByIndex(boundStMock, 10, 99.9, doubleCodec)) shouldBe boundedStMock
    }

    "call to setBytesUnsafe when calling setValueByName[T](BoundStatement, Int, T, ByteBufferCodec[T]) method" in {
      runF(handler.setValueByName(boundStMock, "name", 99.9, doubleCodec)) shouldBe boundedStMock
    }

    "call to bind and setBytesUnsafe when calling setByteBufferListByIndex(PreparedStatement, List[SerializableValueByIndex]) method" in {
      runF(
        handler.setByteBufferListByIndex(
          prepStMock,
          List(SerializableValueBy(10, serializableValue)))) shouldBe boundedStMock
      (prepStMock.bind _).verify()
    }

    "call to bind and setBytesUnsafe when calling setByteBufferListByName(PreparedStatement, List[SerializableValueByName]) method" in {
      runF(
        handler.setByteBufferListByName(
          prepStMock,
          List(SerializableValueBy("name", serializableValue)))) shouldBe boundedStMock
      (prepStMock.bind _).verify()
    }

  }

}
