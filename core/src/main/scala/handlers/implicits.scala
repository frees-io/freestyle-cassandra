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
package handlers

import cats.data.Kleisli
import cats.{~>, MonadError}

import collection.JavaConverters._
import com.datastax.driver.core._
import com.google.common.util.concurrent.{AsyncFunction, Futures, ListenableFuture, MoreExecutors}
import freestyle.async.AsyncContext
import freestyle.cassandra.api.{ClusterAPI, ClusterAPIOps, LowLevelAPI, LowLevelAPIOps}

object implicits {

  import freestyle.cassandra.implicits._

  implicit def lowLevelAPIHandler[M[_]](implicit AC: AsyncContext[M]): LowLevelAPIHandler[M] =
    new LowLevelAPIHandler[M]

  implicit def clusterAPIHandler[M[_]](
      implicit AC: AsyncContext[M],
      E: MonadError[M, Throwable]): ClusterAPIHandler[M] =
    new ClusterAPIHandler[M]

  class LowLevelAPIHandler[M[_]](implicit H: ListenableFuture[?] ~> M)
      extends LowLevelAPI.Handler[LowLevelAPIOps[M, ?]] {

    def init: LowLevelAPIOps[M, Session] = Kleisli(s => H(s.initAsync()))

    def close: LowLevelAPIOps[M, Unit] = closeFuture2unit[M, Session](_.closeAsync())

    def prepare(query: String): LowLevelAPIOps[M, PreparedStatement] =
      Kleisli(s => H(s.prepareAsync(query)))

    def prepareStatement(statement: RegularStatement): LowLevelAPIOps[M, PreparedStatement] =
      Kleisli(s => H(s.prepareAsync(statement)))

    def execute(query: String): LowLevelAPIOps[M, ResultSet] =
      Kleisli(s => H(s.executeAsync(query)))

    def executeWithValues(query: String, values: Any*): LowLevelAPIOps[M, ResultSet] =
      Kleisli(s => H(s.executeAsync(query, values)))

    def executeWithMap(query: String, values: Map[String, AnyRef]): LowLevelAPIOps[M, ResultSet] =
      Kleisli(s => H(s.executeAsync(query, values.asJava)))

    def executeStatement(statement: Statement): LowLevelAPIOps[M, ResultSet] =
      Kleisli(s => H(s.executeAsync(statement)))

  }

  class ClusterAPIHandler[M[_]](implicit H: ListenableFuture[?] ~> M, E: MonadError[M, Throwable])
      extends ClusterAPI.Handler[ClusterAPIOps[M, ?]] {

    def connect: ClusterAPIOps[M, Session] = Kleisli(c => H(c.connectAsync()))

    def connectKeyspace(keyspace: String): ClusterAPIOps[M, Session] =
      Kleisli(c => H(c.connectAsync(keyspace)))

    def close: ClusterAPIOps[M, Unit] = closeFuture2unit[M, Cluster](_.closeAsync())

    def configuration: ClusterAPIOps[M, Configuration] =
      Kleisli(c => E.catchNonFatal(c.getConfiguration))

    def metadata: ClusterAPIOps[M, Metadata] =
      Kleisli(c => E.catchNonFatal(c.getMetadata))

    def metrics: ClusterAPIOps[M, Metrics] =
      Kleisli(c => E.catchNonFatal(c.getMetrics))

  }

  private[this] def closeFuture2unit[M[_], A](f: A => CloseFuture)(
      implicit H: ListenableFuture[?] ~> M): Kleisli[M, A, Unit] = {

    def listenableFuture(a: A): ListenableFuture[Unit] = Futures.transformAsync(
      f(a),
      new AsyncFunction[Void, Unit] {
        override def apply(input: Void): ListenableFuture[Unit] =
          Futures.immediateFuture((): Unit)
      },
      MoreExecutors.directExecutor()
    )

    Kleisli(c => H(listenableFuture(c)))

  }

}
