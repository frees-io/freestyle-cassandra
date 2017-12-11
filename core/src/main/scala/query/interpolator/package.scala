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
package query

import cats.{~>, MonadError}
import com.datastax.driver.core.{ConsistencyLevel, ResultSet, Session}
import contextual.Context
import freestyle.free._
import freestyle.free.implicits._
import freestyle.async.AsyncContext
import freestyle.cassandra.api._
import freestyle.cassandra.query.mapper.FromReader
import freestyle.cassandra.query.model.SerializableValueBy

import scala.concurrent.ExecutionContext

package object interpolator {

  sealed trait CQLContext extends Context
  case object CQLLiteral  extends CQLContext

  case class ParseError(msgList: List[String])
      extends RuntimeException(s"Parse error: ${msgList.mkString(",")}")

  final class InterpolatorOps(tuple: (String, List[SerializableValueBy[Int]])) {

    import freestyle.cassandra.implicits._

    def asResultSet[M[_]](consistencyLevel: Option[ConsistencyLevel] = None)(
        implicit API: SessionAPI[M]): FreeS[M, ResultSet] =
      API.executeWithByteBuffer(tuple._1, tuple._2, consistencyLevel)

    def asFree[M[_]](consistencyLevel: Option[ConsistencyLevel] = None)(
        implicit API: SessionAPI[M]): FreeS[M, Unit] =
      asResultSet[M](consistencyLevel).map(_ => (): Unit)

    def as[A](consistencyLevel: Option[ConsistencyLevel] = None)(
        implicit Q: QueryModule[QueryModule.Op],
        FR: FromReader[A]): FreeS[QueryModule.Op, A] =
      asResultSet[QueryModule.Op](consistencyLevel).flatMap(Q.resultSetAPI.read[A](_))

    def asOption[A](consistencyLevel: Option[ConsistencyLevel] = None)(
        implicit Q: QueryModule[QueryModule.Op],
        FR: FromReader[A]): FreeS[QueryModule.Op, Option[A]] =
      asResultSet[QueryModule.Op](consistencyLevel).flatMap(Q.resultSetAPI.readOption[A](_))

    def asList[A](consistencyLevel: Option[ConsistencyLevel] = None)(
        implicit Q: QueryModule[QueryModule.Op],
        FR: FromReader[A]): FreeS[QueryModule.Op, List[A]] =
      asResultSet[QueryModule.Op](consistencyLevel).flatMap(Q.resultSetAPI.readList[A](_))

    def attemptResultSet[M[_]](consistencyLevel: Option[ConsistencyLevel] = None)(
        implicit API: SessionAPI[SessionAPI.Op],
        S: Session,
        AC: AsyncContext[M],
        E: ExecutionContext,
        ME: MonadError[M, Throwable]): M[ResultSet] =
      asResultSet[SessionAPI.Op](consistencyLevel).interpret[M]

    def attempt[M[_]](consistencyLevel: Option[ConsistencyLevel] = None)(
        implicit API: SessionAPI[SessionAPI.Op],
        S: Session,
        AC: AsyncContext[M],
        E: ExecutionContext,
        ME: MonadError[M, Throwable]): M[Unit] =
      asFree[SessionAPI.Op](consistencyLevel).interpret[M]

    def attemptAs[M[_], A](consistencyLevel: Option[ConsistencyLevel] = None)(
        implicit Q: QueryModule[QueryModule.Op],
        S: Session,
        AC: AsyncContext[M],
        E: ExecutionContext,
        ME: MonadError[M, Throwable],
        FR: FromReader[A]): M[A] =
      as[A](consistencyLevel).interpret[M]

    def attemptAsOption[M[_], A](consistencyLevel: Option[ConsistencyLevel] = None)(
        implicit Q: QueryModule[QueryModule.Op],
        S: Session,
        AC: AsyncContext[M],
        E: ExecutionContext,
        ME: MonadError[M, Throwable],
        FR: FromReader[A]): M[Option[A]] =
      asOption[A](consistencyLevel).interpret[M]

    def attemptAsList[M[_], A](consistencyLevel: Option[ConsistencyLevel] = None)(
        implicit Q: QueryModule[QueryModule.Op],
        S: Session,
        AC: AsyncContext[M],
        E: ExecutionContext,
        ME: MonadError[M, Throwable],
        FR: FromReader[A]): M[List[A]] =
      asList[A](consistencyLevel).interpret[M]

  }

  implicit def interpolatorOps(tuple: (String, List[SerializableValueBy[Int]])): InterpolatorOps =
    new InterpolatorOps(tuple)

}
