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
import cats.data.ValidatedNel
import freestyle.cassandra.schema.provider.SchemaDefinitionProvider

package object validator {

  trait SchemaValidator[M[_]] {

    def validateStatement(sdp: SchemaDefinitionProvider[M], st: Statement)(
        implicit M: MonadError[M, Throwable]): M[ValidatedNel[SchemaError, Unit]]

  }

  object SchemaValidator {

    def apply[M[_]](
        f: (SchemaDefinition, Statement) => M[ValidatedNel[SchemaError, Unit]]): SchemaValidator[M] =
      new SchemaValidator[M]() {

        override def validateStatement(sdp: SchemaDefinitionProvider[M], st: Statement)(
            implicit M: MonadError[M, Throwable]): M[ValidatedNel[SchemaError, Unit]] =
          M.flatMap(sdp.schemaDefinition)(f(_, st))

      }
  }

}
