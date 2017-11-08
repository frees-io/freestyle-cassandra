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

import cats.{~>, MonadError}
import com.datastax.driver.core.{CloseFuture, Cluster, Session}
import com.google.common.util.concurrent.{AsyncFunction, Futures, ListenableFuture}
import freestyle.async.AsyncContext
import freestyle.asyncGuava.implicits._
import freestyle.cassandra.api._
import freestyle.cassandra.handlers._
import java.util.concurrent.{Executor => JavaExecutor}

import scala.concurrent.ExecutionContext

object implicits {

  implicit def clusterAPIInterpreter[M[_]](
      implicit cluster: Cluster,
      AC: AsyncContext[M],
      E: ExecutionContext,
      ME: MonadError[M, Throwable]): ClusterAPI.Op ~> M =
    new ClusterAPIHandler[M] andThen apiInterpreter[M, Cluster](cluster)

  implicit def sessionAPIInterpreter[M[_]](
      implicit session: Session,
      AC: AsyncContext[M],
      E: ExecutionContext,
      ME: MonadError[M, Throwable]): SessionAPI.Op ~> M =
    new SessionAPIHandler[M] andThen apiInterpreter[M, Session](session)

  implicit def statementAPIHandler[M[_]](
      implicit ME: MonadError[M, Throwable]): StatementAPIHandler[M] =
    new StatementAPIHandler[M]

  implicit def resultSetAPIHandler[M[_]](
      implicit ME: MonadError[M, Throwable]): ResultSetAPIHandler[M] =
    new ResultSetAPIHandler[M]

  implicit def closeToListenable(future: CloseFuture)(
      implicit E: ExecutionContext): ListenableFuture[Unit] =
    Futures.transformAsync(
      future,
      new AsyncFunction[Void, Unit] {
        override def apply(input: Void): ListenableFuture[Unit] =
          Futures.immediateFuture((): Unit)
      },
      new JavaExecutor {
        override def execute(command: Runnable): Unit = E.execute(command)
      }
    )

}
