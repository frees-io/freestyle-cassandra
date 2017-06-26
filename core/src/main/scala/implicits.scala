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

import collection.JavaConverters._
import com.datastax.driver.core._
import com.google.common.util.concurrent._
import freestyle._
import freestyle.async.AsyncContext
import freestyle.cassandra.api.LowLevelAPI

object implicits {

  class ListenableFutureHandler(implicit session: Session)
      extends LowLevelAPI.Handler[ListenableFuture] {

    def init: ListenableFuture[Session] = session.initAsync()

    def close: ListenableFuture[Unit] =
      Futures.transformAsync(
        session.closeAsync(),
        new AsyncFunction[Void, Unit] {
          override def apply(input: Void): ListenableFuture[Unit] =
            Futures.immediateFuture((): Unit)
        },
        MoreExecutors.directExecutor()
      )

    def prepare(query: String): ListenableFuture[PreparedStatement] = session.prepareAsync(query)

    def prepareStatement(statement: RegularStatement): ListenableFuture[PreparedStatement] =
      session.prepareAsync(statement)

    def execute(query: String): ListenableFuture[ResultSet] = session.executeAsync(query)

    def executeWithValues(query: String, values: Any*): ListenableFuture[ResultSet] =
      session.executeAsync(query, values)

    def executeWithMap(query: String, values: Map[String, AnyRef]): ListenableFuture[ResultSet] =
      session.executeAsync(query, values.asJava)

    def executeStatement(statement: Statement): ListenableFuture[ResultSet] =
      session.executeAsync(statement)

  }

  class ListenableFuture2AsyncM[M[_]](implicit AC: AsyncContext[M])
      extends FSHandler[ListenableFuture, M] {
    override def apply[A](fa: ListenableFuture[A]): M[A] =
      AC.runAsync { cb =>
        Futures.addCallback(fa, new FutureCallback[A] {
          override def onSuccess(result: A): Unit    = cb(Right(result))
          override def onFailure(t: Throwable): Unit = cb(Left(t))
        })
      }

  }

  implicit def listenableFutureHandler(implicit s: Session): ListenableFutureHandler =
    new ListenableFutureHandler

  implicit def listenableFuture2Async[M[_]](implicit AC: AsyncContext[M]) =
    new ListenableFuture2AsyncM[M]

}
