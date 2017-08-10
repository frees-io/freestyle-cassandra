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

import cats.data.Validated._
import cats.data.{NonEmptyList, ValidatedNel}
import cats.syntax.either._
import freestyle.cassandra.schema.Statement
import freestyle.cassandra.schema.provider.SchemaDefinitionProvider
import troy.schema.{Message, Result, SchemaEngine, V}

class TroySchemaValidator(sdp: SchemaDefinitionProvider) extends SchemaValidator {

  override def validateStatement(st: Statement): ValidatedNel[ValidatorError, Unit] = {

    def toValidatorErrorNel(errors: Seq[Message]): NonEmptyList[ValidatorError] =
      errors.toList match {
        case Nil => NonEmptyList(ValidatorError("Unknown error"), Nil)
        case head :: tail =>
          NonEmptyList(ValidatorError(head.message), tail.map(msg => ValidatorError(msg.message)))
      }

    def parseResult[T](result: Result[T]): ValidatedNel[ValidatorError, T] =
      result match {
        case V.Success(res, _) => Valid(res)
        case V.Error(es, _)    => Invalid(toValidatorErrorNel(es))
      }

    fromEither {
      for {
        schema <- sdp.schemaDefinition.leftMap(e => NonEmptyList(ValidatorError(e.msg), Nil))
        engine <- parseResult(SchemaEngine(schema)).toEither
        _      <- parseResult(engine(st)).toEither
      } yield ()
    }
  }

}
