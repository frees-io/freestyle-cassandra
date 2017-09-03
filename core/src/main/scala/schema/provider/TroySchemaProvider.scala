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
package schema.provider

import java.io.InputStream

import cats.MonadError
import freestyle.cassandra.schema._
import troy.cql.ast.CqlParser

class TroySchemaProvider[M[_]](cqlF: => M[String]) extends SchemaDefinitionProvider[M] {

  override def schemaDefinition(implicit M: MonadError[M, Throwable]): M[SchemaDefinition] =
    M.flatMap(cqlF) { cql =>
      CqlParser.parseSchema(cql) match {
        case CqlParser.Success(res, _) => M.pure(res)
        case CqlParser.Failure(msg, next) =>
          M.raiseError(
            SchemaDefinitionProviderError(
              s"Parse Failure: $msg, line = ${next.pos.line}, column = ${next.pos.column}"))
        case CqlParser.Error(msg, _) => M.raiseError(SchemaDefinitionProviderError(msg))
      }
    }
}

object TroySchemaProvider {

  def apply[M[_]](cql: String)(implicit M: MonadError[M, Throwable]): TroySchemaProvider[M] =
    new TroySchemaProvider(M.pure(cql))

  def apply[M[_]](is: InputStream)(implicit M: MonadError[M, Throwable]): TroySchemaProvider[M] =
    new TroySchemaProvider[M](
      catchNonFatalAsSchemaError(scala.io.Source.fromInputStream(is).mkString))

}
