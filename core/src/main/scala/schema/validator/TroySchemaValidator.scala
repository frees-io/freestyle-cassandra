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
package schema.validator

import cats.data.NonEmptyList
import cats.syntax.either._
import freestyle.cassandra.schema._
import troy.schema.{Message, Result, SchemaEngine, V}

object TroySchemaValidator {

  def validateStatement(
      schema: SchemaDefinition,
      st: Statement): Either[NonEmptyList[SchemaError], Unit] = {

    implicit class MessageOps(message: Message) {

      def toSchemaValidatorError: SchemaValidatorError = SchemaValidatorError(message.message)

    }

    implicit class ResultOps[T](result: Result[T]) {

      def toEither: Either[Seq[Message], T] =
        result match {
          case V.Success(res, _) => res.asRight
          case V.Error(es, _)    => es.asLeft
        }

    }

    SchemaEngine(schema)
      .flatMap(schemaEngine => schemaEngine(st))
      .map(_ => (): Unit)
      .toEither
      .leftMap { seq =>
        NonEmptyList
          .fromList(seq.map(_.toSchemaValidatorError).toList)
          .getOrElse(NonEmptyList(SchemaValidatorError("Unknown error"), Nil))
      }
  }

  implicit val instance: SchemaValidator = SchemaValidator(validateStatement)

}
