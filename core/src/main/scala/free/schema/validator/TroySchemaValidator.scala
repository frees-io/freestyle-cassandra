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

package freestyle.free.cassandra
package schema.validator

import cats.MonadError
import cats.data.{NonEmptyList, Validated, ValidatedNel}
import freestyle.free.cassandra.schema._
import freestyle.free.cassandra.schema.provider.SchemaDefinitionProvider
import troy.cql.ast.DataDefinition
import troy.schema.{Message, Result, SchemaEngine, V}

class TroySchemaValidator[M[_]](
    implicit E: MonadError[M, Throwable],
    SDP: SchemaDefinitionProvider[M])
    extends SchemaValidator[M] {

  override def validateStatement(st: Statements)(
      implicit E: MonadError[M, Throwable]): M[ValidatedNel[SchemaError, Unit]] = {

    def toSchemaValidatorError(message: Message): SchemaValidatorError =
      SchemaValidatorError(message.message)

    def toValidatedNel[T](result: Result[T]): ValidatedNel[SchemaError, T] =
      result match {
        case V.Success(res, _) => Validated.valid(res)
        case V.Error(es, _) =>
          Validated.invalid(
            NonEmptyList
              .fromList(es.map(toSchemaValidatorError).toList)
              .getOrElse(NonEmptyList(SchemaValidatorError("Unknown error"), Nil)))
      }

    def validateStatement(
        schema: Seq[DataDefinition],
        st: Statements): M[ValidatedNel[SchemaError, Unit]] = {

      type ValidatedNelSchemaError[T] = ValidatedNel[SchemaError, T]

      def validateWithSchema(schemaEngine: SchemaEngine): Result[Unit] =
        (st match {
          case DML(sts) => schemaEngine(sts)
          case DDL(sts) => SchemaEngine(schema ++ sts)
        }).map(_ => (): Unit)

      catchNonFatalAsSchemaError {
        toValidatedNel {
          SchemaEngine(schema).flatMap(validateWithSchema)
        }
      }
    }

    E.flatMap(SDP.schemaDefinition)(validateStatement(_, st))
  }

}

object TroySchemaValidator {

  implicit def instance[M[_]](
      implicit E: MonadError[M, Throwable],
      SDP: SchemaDefinitionProvider[M]): SchemaValidator[M] = new TroySchemaValidator[M]
}
