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

import cats.data.Kleisli
import cats.{~>, MonadError}
import com.datastax.driver.core.{CloseFuture, Cluster, ResultSet, Session}
import com.google.common.util.concurrent.{AsyncFunction, Futures, ListenableFuture, MoreExecutors}

import scala.reflect.ClassTag

package object api {

  type SessionAPIOps[F[_], A] = Kleisli[F, Session, A]

  type ClusterAPIOps[F[_], A] = Kleisli[F, Cluster, A]

  type ResultSetAPIOps[F[_], A] = Kleisli[F, ResultSet, A]

  def apiInterpreter[F[_], A](a: A): (Kleisli[F, A, ?] ~> F) = new (Kleisli[F, A, ?] ~> F) {
    override def apply[B](fa: Kleisli[F, A, B]): F[B] = fa(a)
  }

  def kleisli[M[_], A, B](
      f: A => M[B])(implicit ME: MonadError[M, Throwable], TAG: ClassTag[A]): Kleisli[M, A, B] =
    Kleisli { (a: A) =>
      Option(a)
        .map(f)
        .getOrElse(ME.raiseError(
          new IllegalArgumentException(s"Instance of class ${TAG.runtimeClass.getName} is null")))
    }

  def closeFuture2unit[M[_], A](f: A => CloseFuture)(
      implicit H: ListenableFuture[?] ~> M,
      ME: MonadError[M, Throwable],
      TAG: ClassTag[A]): Kleisli[M, A, Unit] = {

    def listenableFuture(a: A): ListenableFuture[Unit] = Futures.transformAsync(
      f(a),
      new AsyncFunction[Void, Unit] {
        override def apply(input: Void): ListenableFuture[Unit] =
          Futures.immediateFuture((): Unit)
      },
      MoreExecutors.directExecutor()
    )

    kleisli(c => H(listenableFuture(c)))

  }

}
