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

package freestyle
package cassandra
package api

import cats.syntax.either._
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}
import freestyle.async._

class GuavaUtils[F[_]: AsyncM] {

  def call[I, O](f: ListenableFuture[I], map: I => O): FreeS[F, O] =
    AsyncM[F].async[O] { cb =>
      Futures.addCallback(f, new FutureCallback[I] {
        override def onSuccess(result: I): Unit    = cb(map(result).asRight)
        override def onFailure(t: Throwable): Unit = cb(t.asLeft)
      })
    }

  def call[T](f: ListenableFuture[T]): FreeS[F, T] = call[T, T](f, v => v)

}
