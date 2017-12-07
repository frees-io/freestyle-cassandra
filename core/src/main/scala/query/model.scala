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

import java.nio.ByteBuffer

import cats.MonadError

object model {

  trait SerializableValue {
    def serialize[M[_]](implicit E: MonadError[M, Throwable]): M[ByteBuffer]
  }

  trait SerializableValueBy[T] {
    def position: T
    def serializableValue: SerializableValue
  }

  object SerializableValueBy {

    def apply(p: Int, s: SerializableValue): SerializableValueBy[Int] =
      new SerializableValueBy[Int] {
        override def position: Int                        = p
        override def serializableValue: SerializableValue = s
      }

    def apply(p: String, s: SerializableValue): SerializableValueBy[String] =
      new SerializableValueBy[String] {
        override def position: String                     = p
        override def serializableValue: SerializableValue = s
      }
  }
}
