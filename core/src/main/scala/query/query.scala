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

import java.nio.ByteBuffer

import cats.MonadError

package object query {

  trait ByteBufferReader {
    def read[M[_]](name: String)(implicit ME: MonadError[M, Throwable]): M[ByteBuffer]
  }

  trait Printer {
    def print(name: String): String
  }

  object Printer {
    def apply(f: String => String): Printer = new Printer {
      override def print(name: String): String = f(name)
    }
  }

  val identityPrinter: Printer = Printer(identity)

  val lowerCasePrinter: Printer = Printer(_.toLowerCase)

  val upperCasePrinter: Printer = Printer(_.toUpperCase)

}
