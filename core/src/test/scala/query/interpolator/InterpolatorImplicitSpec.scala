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

import cats.{~>, MonadError}
import com.datastax.driver.core._
import freestyle.cassandra.api.{apiInterpreter, SessionAPI}
import freestyle.cassandra.codecs.ByteBufferCodec
import freestyle.cassandra.handlers.implicits.sessionAPIHandler
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

class InterpolatorImplicitSpec
    extends WordSpec
    with Matchers
    with OneInstancePerTest
    with MockFactory {

  import RuntimeCQLInterpolator._
  import freestyle.cassandra.query.interpolator._

  import freestyle.async.implicits._
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val E: MonadError[Future, Throwable] =
    cats.instances.future.catsStdInstancesForFuture

  implicit val sessionMock: Session = stub[Session]

  val rsMock: ResultSet = stub[ResultSet]
  (sessionMock.executeAsync(_: Statement)).when(*).returns(ResultSetFutureTest(rsMock))

  val consistencyLevel = ConsistencyLevel.EACH_QUORUM

  "InterpolatorImplicitDef asResultSet" should {

    implicit val interpreter = sessionAPIHandler[Future] andThen apiInterpreter[Future, Session](
      sessionMock)

    "return a valid ResultSet from a FreeS" in {
      val future: Future[ResultSet] =
        cql"SELECT * FROM users".asResultSet[SessionAPI.Op]().interpret[Future]
      Await.result(future, Duration.Inf) shouldBe rsMock
    }

    "return a valid ResultSet from a FreeS when passing a ConsistencyLevel" in {
      val future: Future[ResultSet] =
        cql"SELECT * FROM users"
          .asResultSet[SessionAPI.Op](Some(consistencyLevel))
          .interpret[Future]
      Await.result(future, Duration.Inf) shouldBe rsMock
      (sessionMock
        .executeAsync(_: Statement))
        .verify(where { (st: Statement) => st.getConsistencyLevel == consistencyLevel
        })
    }

    "return a failed future when the ByteBufferCodec returns a failure" in {
      val serializeException = new RuntimeException("Error serializing")
      implicit val stringByteBufferCodec: ByteBufferCodec[String] = new ByteBufferCodec[String] {
        override def deserialize[M[_]](bytes: ByteBuffer)(
            implicit E: MonadError[M, Throwable]): M[String] =
          E.raiseError(new RuntimeException("Error deserializing"))

        override def serialize[M[_]](value: String)(
            implicit E: MonadError[M, Throwable]): M[ByteBuffer] =
          E.raiseError(serializeException)
      }
      val name: String = "UserName"
      val future: Future[ResultSet] =
        cql"SELECT * FROM users WHERE name=$name".asResultSet[SessionAPI.Op]().interpret[Future]
      Await.result(future.failed, Duration.Inf) shouldBe serializeException
    }

    "return a failed future when the ByteBufferCodec returns a failure when passing a ConsistencyLevel" in {
      val serializeException = new RuntimeException("Error serializing")
      implicit val stringByteBufferCodec: ByteBufferCodec[String] = new ByteBufferCodec[String] {
        override def deserialize[M[_]](bytes: ByteBuffer)(
            implicit E: MonadError[M, Throwable]): M[String] =
          E.raiseError(new RuntimeException("Error deserializing"))

        override def serialize[M[_]](value: String)(
            implicit E: MonadError[M, Throwable]): M[ByteBuffer] =
          E.raiseError(serializeException)
      }
      val name: String = "UserName"
      val future: Future[ResultSet] =
        cql"SELECT * FROM users WHERE name=$name"
          .asResultSet[SessionAPI.Op](Some(consistencyLevel))
          .interpret[Future]
      Await.result(future.failed, Duration.Inf) shouldBe serializeException
    }

  }

  "InterpolatorImplicitDef attemptResultSet()" should {

    "return a valid ResultSet" in {
      val future: Future[ResultSet] = cql"SELECT * FROM users".attemptResultSet[Future]()
      Await.result(future, Duration.Inf) shouldBe rsMock
    }

    "return a valid ResultSet when passing a ConsistencyLevel" in {
      val future: Future[ResultSet] =
        cql"SELECT * FROM users".attemptResultSet[Future](Some(consistencyLevel))
      Await.result(future, Duration.Inf) shouldBe rsMock
      (sessionMock
        .executeAsync(_: Statement))
        .verify(where { (st: Statement) => st.getConsistencyLevel == consistencyLevel
        })
    }

    "return a failed future when the ByteBufferCodec returns a failure" in {
      val serializeException = new RuntimeException("Error serializing")
      implicit val stringByteBufferCodec: ByteBufferCodec[String] = new ByteBufferCodec[String] {
        override def deserialize[M[_]](bytes: ByteBuffer)(
            implicit E: MonadError[M, Throwable]): M[String] =
          E.raiseError(new RuntimeException("Error deserializing"))

        override def serialize[M[_]](value: String)(
            implicit E: MonadError[M, Throwable]): M[ByteBuffer] =
          E.raiseError(serializeException)
      }
      val name: String = "UserName"
      val future: Future[ResultSet] =
        cql"SELECT * FROM users WHERE name=$name".attemptResultSet[Future]()
      Await.result(future.failed, Duration.Inf) shouldBe serializeException
    }

    "return a failed future when the ByteBufferCodec returns a failure when passing a ConsistencyLevel" in {
      val serializeException = new RuntimeException("Error serializing")
      implicit val stringByteBufferCodec: ByteBufferCodec[String] = new ByteBufferCodec[String] {
        override def deserialize[M[_]](bytes: ByteBuffer)(
            implicit E: MonadError[M, Throwable]): M[String] =
          E.raiseError(new RuntimeException("Error deserializing"))

        override def serialize[M[_]](value: String)(
            implicit E: MonadError[M, Throwable]): M[ByteBuffer] =
          E.raiseError(serializeException)
      }
      val name: String = "UserName"
      val future: Future[ResultSet] =
        cql"SELECT * FROM users WHERE name=$name".attemptResultSet[Future](Some(consistencyLevel))
      Await.result(future.failed, Duration.Inf) shouldBe serializeException
    }

  }

}
