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

import cats.{Applicative, MonadError}
import troy.cql.ast.{Cql3Statement, DataDefinition, DataManipulation}
import troy.schema.{Result, V}

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

  sealed trait Statements {
    def statements: Seq[Cql3Statement]
  }
  case class DefinitionStatements(statements: Seq[DataDefinition])     extends Statements
  case class ManipulationStatements(statements: Seq[DataManipulation]) extends Statements
  object ManipulationStatements {
    def apply(statement: DataManipulation): ManipulationStatements =
      new ManipulationStatements(Seq(statement))
  }

  def catchNonFatalAsSchemaError[M[_], A](value: => A)(implicit E: MonadError[M, Throwable]): M[A] =
    E.handleErrorWith {
      E.catchNonFatal(value)
    }(e => E.raiseError(SchemaDefinitionProviderError(e)))

  implicit def resultApplicative: Applicative[Result] = new Applicative[Result] {

    override def pure[A](x: A): Result[A] = V.success(x)

    override def ap[A, B](ff: Result[A => B])(fa: Result[A]): Result[B] =
      fa.flatMap(a => ff.map(_(a)))
  }

}
