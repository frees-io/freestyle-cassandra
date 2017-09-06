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

import cats.MonadError
import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.instances.either._
import freestyle.cassandra.TestUtils.EitherM
import freestyle.cassandra.schema.provider.SchemaDefinitionProvider
import freestyle.cassandra.schema._
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, WordSpec}
import troy.cql.ast.{SelectStatement, TableName}
import troy.cql.ast.dml.Select

class SchemaValidatorSpec extends WordSpec with Matchers with MockFactory {

  val mockStatement: Statement = SelectStatement(
    mod = None,
    selection = Select.SelectClause(Seq.empty),
    from = TableName(None, ""),
    where = None,
    orderBy = None,
    perPartitionLimit = None,
    limit = None,
    allowFiltering = false
  )

  "apply method" should {

    "return Unit if the schema provider and the provided function works as expected" in {
      val sv: SchemaValidator[EitherM] = new SchemaValidator[EitherM] {
        override def validateStatement(st: Statement)(
            implicit M: MonadError[EitherM, Throwable]): Either[
          Throwable,
          ValidatedNel[SchemaError, Unit]] = Right(Validated.valid((): Unit))
      }

      sv.validateStatement(mockStatement) shouldBe Right(Validated.valid((): Unit))
    }

    "return an error if the schema provider return an error" in {

      val exc = SchemaDefinitionProviderError("Test error")
      val sv: SchemaValidator[EitherM] = new SchemaValidator[EitherM] {
        override def validateStatement(st: Statement)(
            implicit M: MonadError[EitherM, Throwable]): Either[
          Throwable,
          ValidatedNel[SchemaError, Unit]] = Left(exc)
      }

      sv.validateStatement(mockStatement) shouldBe Left(exc)
    }

  }

}
