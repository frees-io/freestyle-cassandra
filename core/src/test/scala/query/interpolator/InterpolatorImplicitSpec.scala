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
import java.util.UUID

import cats.MonadError
import com.datastax.driver.core._
import freestyle.free._
import freestyle.free.implicits._
import freestyle.async.implicits._
import freestyle.cassandra.TestData._
import freestyle.cassandra.TestUtils._
import freestyle.cassandra.codecs.ByteBufferCodec
import freestyle.cassandra.config.ConfigArbitraries._
import freestyle.cassandra.implicits._
import freestyle.cassandra.query.FieldLister._
import freestyle.cassandra.query.mapper.FieldListMapper._
import freestyle.cassandra.query._
import freestyle.cassandra.query.QueryArbitraries._
import freestyle.cassandra.query.mapper.FromReader
import freestyle.cassandra.query.mapper.GenericFromReader._
import org.scalacheck.Gen
import org.scalacheck.Prop._
import org.scalamock.scalatest.MockFactory
import org.scalatest.prop.Checkers
import org.scalatest.{OneInstancePerTest, WordSpec}

import scala.concurrent.Future

class InterpolatorImplicitSpec
    extends WordSpec
    with MatchersUtil
    with Checkers
    with OneInstancePerTest
    with MockFactory {

  import RuntimeCQLInterpolator._

  import freestyle.free.async.implicits._
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val E: MonadError[Future, Throwable] =
    cats.instances.future.catsStdInstancesForFuture

  val rsMock: ResultSet = stub[ResultSet]

  def mockSession(cl: Option[ConsistencyLevel], resultSet: ResultSet = rsMock): Session = {
    val sessionMock: Session = mock[Session]
    (sessionMock
      .executeAsync(_: Statement))
      .expects(where((st: Statement) => cl.forall(v => st.getConsistencyLevel == v)))
      .returns(ResultSetFutureTest(resultSet))
    sessionMock
  }

  implicit val printer: Printer = identityPrinter

  implicit val protocolVersion: ProtocolVersion   = ProtocolVersion.V3
  implicit val stringTypeCodec: TypeCodec[String] = TypeCodec.ascii()
  implicit val uuidTypeCodec: TypeCodec[UUID]     = TypeCodec.uuid()

  val reader: FromReader[User] = implicitly[FromReader[User]]

  "InterpolatorImplicitDef asResultSet" should {

    "return a valid ResultSet from a FreeS" in {
      check {
        forAll { cl: Option[ConsistencyLevel] =>
          implicit val sessionMock: Session = mockSession(cl)

          val future: Future[ResultSet] =
            cql"CREATE TABLE users (id uuid PRIMARY KEY)".asResultSet(cl).interpret[Future]
          runF(future) isEqualTo rsMock
        }
      }
    }

    "return a failed future when the ByteBufferCodec returns a failure" in {
      check {
        forAll { cl: Option[ConsistencyLevel] =>
          implicit val sessionMock: Session = mock[Session]
          val serializeException            = new RuntimeException("Error serializing")
          implicit val stringByteBufferCodec: ByteBufferCodec[String] =
            new ByteBufferCodec[String] {
              override def deserialize[M[_]](bytes: ByteBuffer)(
                  implicit E: MonadError[M, Throwable]): M[String] =
                E.raiseError(new RuntimeException("Error deserializing"))

              override def serialize[M[_]](value: String)(
                  implicit E: MonadError[M, Throwable]): M[ByteBuffer] =
                E.raiseError(serializeException)
            }
          val name: String = "UserName"
          val future: Future[ResultSet] =
            cql"SELECT * FROM users WHERE name=$name".asResultSet().interpret[Future]
          runF(future.failed) isEqualTo serializeException
        }
      }
    }
  }

  "InterpolatorImplicitDef asFree" should {

    "return a valid result from a FreeS" in {

      check {
        forAll { cl: Option[ConsistencyLevel] =>
          implicit val sessionMock: Session = mockSession(cl)

          val future: Future[Unit] =
            cql"CREATE TABLE users (id uuid PRIMARY KEY)".asFree(cl).interpret[Future]
          runF(future) isEqualTo ((): Unit)
        }
      }
    }

  }

  "InterpolatorImplicitDef as[A]" should {

    "return a valid result from a FreeS" in {

      check {
        forAll(rowAndDataGen[User], Gen.option(consistencyLevelArb.arbitrary)) {
          case ((resultSet, list), cl) =>
            implicit val sessionMock: Session = mockSession(cl, resultSet)
            val future: Future[User] =
              cql"SELECT * FROM users".as[User](cl).interpret[Future]
            if (list.isEmpty) {
              runFFailed(future) isLikeTo (_.isInstanceOf[IllegalStateException])
            } else {
              runF(future) isEqualTo list.head
            }
        }
      }
    }

  }

  "InterpolatorImplicitDef asOption[A]" should {

    "return a valid result from a FreeS" in {

      check {
        forAll(rowAndDataGen[User], Gen.option(consistencyLevelArb.arbitrary)) {
          case ((resultSet, list), cl) =>
            implicit val sessionMock: Session = mockSession(cl, resultSet)
            val future: Future[Option[User]] =
              cql"SELECT * FROM users".asOption[User](cl).interpret[Future]
            runF(future) isEqualTo list.headOption
        }
      }
    }

  }

  "InterpolatorImplicitDef asList[A]" should {

    "return a valid result from a FreeS" in {

      check {
        forAll(rowAndDataGen[User], Gen.option(consistencyLevelArb.arbitrary)) {
          case ((resultSet, list), cl) =>
            implicit val sessionMock: Session = mockSession(cl, resultSet)
            val future: Future[List[User]] =
              cql"SELECT * FROM users".asList[User](cl).interpret[Future]
            runF(future) isEqualTo list
        }
      }
    }

  }

  "InterpolatorImplicitDef attemptResultSet()" should {

    "return a valid ResultSet" in {
      check {
        forAll { cl: Option[ConsistencyLevel] =>
          implicit val sessionMock: Session = mockSession(cl)

          val future: Future[ResultSet] = cql"SELECT * FROM users".attemptResultSet[Future](cl)
          runF(future) isEqualTo rsMock
        }
      }
    }

    "return a failed future when the ByteBufferCodec returns a failure" in {
      check {
        forAll { cl: Option[ConsistencyLevel] =>
          implicit val sessionMock: Session = mock[Session]

          val serializeException = new RuntimeException("Error serializing")
          implicit val stringByteBufferCodec: ByteBufferCodec[String] =
            new ByteBufferCodec[String] {
              override def deserialize[M[_]](bytes: ByteBuffer)(
                  implicit E: MonadError[M, Throwable]): M[String] =
                E.raiseError(new RuntimeException("Error deserializing"))

              override def serialize[M[_]](value: String)(
                  implicit E: MonadError[M, Throwable]): M[ByteBuffer] =
                E.raiseError(serializeException)
            }
          val name: String = "UserName"
          val future: Future[ResultSet] =
            cql"SELECT * FROM users WHERE name=$name".attemptResultSet[Future](cl)
          runFFailed(future) isEqualTo serializeException
        }
      }
    }

  }

  "InterpolatorImplicitDef attempt()" should {

    "return a valid result" in {

      check {
        forAll { cl: Option[ConsistencyLevel] =>
          implicit val sessionMock: Session = mockSession(cl)

          val future: Future[Unit] =
            cql"CREATE TABLE users (id uuid PRIMARY KEY)".attempt[Future](cl)
          runF(future) isEqualTo ((): Unit)
        }
      }
    }

  }

  "InterpolatorImplicitDef attemptAs[A]" should {

    "return a valid result from a FreeS" in {

      check {
        forAll(rowAndDataGen[User], Gen.option(consistencyLevelArb.arbitrary)) {
          case ((resultSet, list), cl) =>
            implicit val sessionMock: Session = mockSession(cl, resultSet)
            val future: Future[User] =
              cql"SELECT * FROM users".attemptAs[Future, User](cl)
            if (list.isEmpty) {
              runFFailed(future) isLikeTo (_.isInstanceOf[IllegalStateException])
            } else {
              runF(future) isEqualTo list.head
            }
        }
      }
    }

  }

  "InterpolatorImplicitDef attemptAsOption[A]" should {

    "return a valid result from a FreeS" in {

      check {
        forAll(rowAndDataGen[User], Gen.option(consistencyLevelArb.arbitrary)) {
          case ((resultSet, list), cl) =>
            implicit val sessionMock: Session = mockSession(cl, resultSet)
            val future: Future[Option[User]] =
              cql"SELECT * FROM users".attemptAsOption[Future, User](cl)
            runF(future) isEqualTo list.headOption
        }
      }
    }

  }

  "InterpolatorImplicitDef attemptAsList[A]" should {

    "return a valid result from a FreeS" in {

      check {
        forAll(rowAndDataGen[User], Gen.option(consistencyLevelArb.arbitrary)) {
          case ((resultSet, list), cl) =>
            implicit val sessionMock: Session = mockSession(cl, resultSet)
            val future: Future[List[User]] =
              cql"SELECT * FROM users".attemptAsList[Future, User](cl)
            runF(future) isEqualTo list
        }
      }
    }

  }

}
