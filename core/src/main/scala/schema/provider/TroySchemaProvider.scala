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

import cats.syntax.either._
import freestyle.cassandra.schema._
import troy.cql.ast.CqlParser

class TroySchemaProvider(cqlF: => SchemaResult[String]) extends SchemaDefinitionProvider {

  override def schemaDefinition: SchemaResult[SchemaDefinition] =
    cqlF.flatMap { cql =>
      CqlParser.parseSchema(cql) match {
        case CqlParser.Success(res, _) => Right(res)
        case CqlParser.Failure(msg, next) =>
          Left(
            SchemaDefinitionProviderError(
              s"Parse Failure: $msg, line = ${next.pos.line}, column = ${next.pos.column}"))
        case CqlParser.Error(msg, _) => Left(SchemaDefinitionProviderError(msg))
      }
    }
}

object TroySchemaProvider {

  def apply(cql: String): TroySchemaProvider = new TroySchemaProvider(Right(cql))

  def apply(is: InputStream): TroySchemaProvider = new TroySchemaProvider(
    Either.catchNonFatal {
      scala.io.Source.fromInputStream(is).mkString
    } leftMap { e =>
      SchemaDefinitionProviderError(e.getMessage, Some(e))
    }
  )

}
