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
import freestyle._
import freestyle.async.AsyncContext
import freestyle.cassandra.api.{apiInterpreter, SessionAPI}
import freestyle.cassandra.query.model.SerializableValueBy

package object interpolator {

  sealed trait CQLContext extends Context
  case object CQLLiteral  extends CQLContext

  final class InterpolatorOps(tuple: (String, List[SerializableValueBy[Int]])) {

    import freestyle.implicits._
    import freestyle.cassandra.handlers.implicits._

    implicit def sessionAPIInterpreter[M[_]](
        implicit S: Session,
        AC: AsyncContext[M],
        E: MonadError[M, Throwable]): SessionAPI.Op ~> M =
      sessionAPIHandler andThen apiInterpreter[M, Session](S)

    def asResultSet[M[_]](implicit API: SessionAPI[M]): FreeS[M, ResultSet] =
      API.executeWithByteBuffer(tuple._1, tuple._2)

    def asResultSet[M[_]](consistencyLevel: ConsistencyLevel)(
        implicit API: SessionAPI[M]): FreeS[M, ResultSet] =
      API.executeWithByteBufferAndCL(tuple._1, tuple._2, consistencyLevel)

    def attemptResultSet[M[_]](
        implicit API: SessionAPI[SessionAPI.Op],
        S: Session,
        AC: AsyncContext[M],
        E: MonadError[M, Throwable]): M[ResultSet] =
      asResultSet[SessionAPI.Op].interpret[M]

    def attemptResultSet[M[_]](consistencyLevel: ConsistencyLevel)(
        implicit API: SessionAPI[SessionAPI.Op],
        S: Session,
        AC: AsyncContext[M],
        E: MonadError[M, Throwable]): M[ResultSet] =
      asResultSet[SessionAPI.Op](consistencyLevel).interpret[M]

  }

  implicit def inserpolatorOps(tuple: (String, List[SerializableValueBy[Int]])): InterpolatorOps =
    new InterpolatorOps(tuple)

}
