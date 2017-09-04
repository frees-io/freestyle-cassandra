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

import contextual.{Case, Prefix}
import freestyle.cassandra.query.{CQLInterpolator, CQLLiteral}

object MyInterpolator {

  import freestyle.cassandra.query.TroySchemaCQLInterpolator._

  object cqlInterpolator extends CQLInterpolator(troySchemaValidator)

  implicit def embedArgsNamesInCql[T] = cqlInterpolator.embed[T](
    Case(CQLLiteral, CQLLiteral)(_ => "?")
  )

  final class CQLStringContext(sc: StringContext) {
    val cql = Prefix(cqlInterpolator, sc)
  }

  implicit def cqlStringContext(sc: StringContext): CQLStringContext =
    new CQLStringContext(sc)

}
