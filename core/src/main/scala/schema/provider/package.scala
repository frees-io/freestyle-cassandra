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
package schema

import cats.MonadError

package object provider {

  trait SchemaDefinitionProvider[M[_]] {
    def schemaDefinition(implicit E: MonadError[M, Throwable]): M[SchemaDefinition]
  }

  def guarantee[M[_], A](fa: => A, finalizer: => Unit)(implicit E: MonadError[M, Throwable]): M[A] =
    E.flatMap(E.attempt(catchNonFatalAsSchemaError(fa))) { e =>
      E.flatMap(catchNonFatalAsSchemaError(finalizer))(_ => e.fold(E.raiseError, E.pure))
    }

}
