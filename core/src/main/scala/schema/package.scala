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

import cats.MonadError
import troy.cql.ast.{DataDefinition, DataManipulation}

package object schema {

  sealed abstract class SchemaError(msg: String, maybeCause: Option[Throwable] = None)
      extends RuntimeException(msg) {
    maybeCause foreach initCause
  }

  case class SchemaDefinitionProviderError(msg: String, maybeCause: Option[Throwable] = None)
      extends SchemaError(msg, maybeCause)

  object SchemaDefinitionProviderError {
    def apply(e: Throwable): SchemaDefinitionProviderError =
      SchemaDefinitionProviderError(e.getMessage, Some(e))
  }

  case class SchemaValidatorError(msg: String, maybeCause: Option[Throwable] = None)
      extends SchemaError(msg, maybeCause)

  type SchemaDefinition = Seq[DataDefinition]
  type Statement        = DataManipulation

  def catchNonFatalAsSchemaError[M[_], A](value: => A)(implicit E: MonadError[M, Throwable]): M[A] =
    E.handleErrorWith(E.catchNonFatal(value)) { e => E.raiseError(SchemaDefinitionProviderError(e))
    }

}
