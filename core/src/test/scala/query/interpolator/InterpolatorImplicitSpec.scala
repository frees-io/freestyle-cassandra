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
package query.interpolator

import java.nio.ByteBuffer

import cats.MonadError
import com.datastax.driver.core._
import com.google.common.util.concurrent.ListenableFuture
import freestyle.cassandra.TestUtils._
import freestyle.cassandra.codecs.ByteBufferCodec
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

class InterpolatorImplicitSpec
    extends WordSpec
    with Matchers
    with OneInstancePerTest
    with MockFactory {

  implicit val sessionMock: Session = stub[Session]

  val prepStMock: PreparedStatement = stub[PreparedStatement]
  (prepStMock.getVariables _).when().returns(ColumnDefinitionsTest)
  (prepStMock.getPreparedId _).when().returns(PreparedIdTest)

  val rsMock: ResultSet = stub[ResultSet]

  (sessionMock.executeAsync(_: Statement)).when(*).returns(ResultSetFutureTest(rsMock))

  val boundSt: BoundStatement = new BoundStatement(prepStMock)
  (prepStMock.bind _).when().returns(boundSt)

  "InterpolatorImplicitDef asResultSet" should {

    import RuntimeCQLInterpolator._
    import freestyle.async.implicits._
    import freestyle.cassandra.query.interpolator._
    import scala.concurrent.ExecutionContext.Implicits.global

    implicit val E: MonadError[Future, Throwable] =
      cats.instances.future.catsStdInstancesForFuture

    "return a valid ResultSet" in {
      val listenableSt: ListenableFuture[PreparedStatement] = successfulFuture(prepStMock)
      (sessionMock.prepareAsync(_: String)).when(*).returns(listenableSt)
      val future: Future[ResultSet] = cql"SELECT * FROM users".asResultSet[Future]
      Await.result(future, Duration.Inf) shouldBe rsMock
    }

    "return a failed future when the session returns a failed future" in {
      val listenableSt: ListenableFuture[PreparedStatement] = failedFuture[PreparedStatement]
      (sessionMock.prepareAsync(_: String)).when(*).returns(listenableSt)
      val future: Future[ResultSet] = cql"SELECT * FROM users".asResultSet[Future]
      Await.result(future.failed, Duration.Inf) shouldBe exception
    }

    "return a failed future when the ByteBufferCodec returns a failure" in {
      val listenableSt: ListenableFuture[PreparedStatement] = successfulFuture(prepStMock)
      (sessionMock.prepareAsync(_: String)).when(*).returns(listenableSt)
      val serializeException = new RuntimeException("Error serializing")
      implicit val stringByteBufferCodec: ByteBufferCodec[String] = new ByteBufferCodec[String] {
        override def deserialize[M[_]](bytes: ByteBuffer)(
            implicit E: MonadError[M, Throwable]): M[String] =
          E.raiseError(new RuntimeException("Error deserializing"))

        override def serialize[M[_]](value: String)(
            implicit E: MonadError[M, Throwable]): M[ByteBuffer] =
          E.raiseError(serializeException)
      }
      val name: String              = "UserName"
      val future: Future[ResultSet] = cql"SELECT * FROM users WHERE name=$name".asResultSet[Future]
      Await.result(future.failed, Duration.Inf) shouldBe serializeException
    }

  }

}
