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

import cats.MonadError
import cats.data.Validated.Valid
import cats.data.ValidatedNel
import contextual.{Case, Prefix}
import freestyle.cassandra.codecs.ByteBufferCodec
import freestyle.cassandra.schema.validator.SchemaValidator
import freestyle.cassandra.schema.{SchemaError, Statement}

import scala.util.{Success, Try}

object RuntimeCQLInterpolator {

  private[this] val schemaValidator: SchemaValidator[Try] = new SchemaValidator[Try] {
    override def validateStatement(st: Statement)(
        implicit M: MonadError[Try, Throwable]): Try[ValidatedNel[SchemaError, Unit]] =
      Success(Valid((): Unit))
  }

  object cqlInterpolator extends CQLInterpolator(schemaValidator)

  implicit def embedArgsNamesInCql[T](implicit C: ByteBufferCodec[T]) = cqlInterpolator.embed[T](
    Case(CQLLiteral, CQLLiteral) { v =>
      new ValueEncoder {
        override def encode[M[_]](implicit M: MonadError[M, Throwable]): M[ByteBuffer] =
          C.serialize(v)
      }
    }
  )

  final class CQLStringContext(sc: StringContext) {
    val cql = Prefix(cqlInterpolator, sc)
  }

  implicit def cqlStringContext(sc: StringContext): CQLStringContext =
    new CQLStringContext(sc)

}
