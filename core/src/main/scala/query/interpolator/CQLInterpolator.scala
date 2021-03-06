/*
 * Copyright 2017-2018 47 Degrees, LLC. <http://www.47deg.com>
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

import cats.data.Validated.{Invalid, Valid}
import contextual.Interpolator
import freestyle.cassandra.query.model.{SerializableValue, SerializableValueBy}
import freestyle.cassandra.schema.{DDL, DML, Statements}
import freestyle.cassandra.schema.validator.SchemaValidator
import troy.cql.ast.{CqlParser, DataDefinition, DataManipulation}

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

class CQLInterpolator(V: SchemaValidator[Try]) extends Interpolator {

  override type ContextType = CQLContext
  override type Input       = SerializableValue

  override def contextualize(interpolation: StaticInterpolation): Seq[ContextType] = {

    val cql = interpolation.parts.foldLeft("") {
      case (prev, _ @Literal(_, string)) => prev + string
      case (prev, _ @Hole(_, _))         => prev + "?"
      case (prev, _)                     => prev
    }

    parseStatement(cql).flatMap(V.validateStatement(_)(cats.instances.try_.catsStdInstancesForTry)) match {
      case Success(Valid(_)) =>
        Seq.fill(interpolation.parts.size)(CQLLiteral)
      case Success(Invalid(list)) =>
        interpolation.abort(Literal(0, cql), 0, list.map(_.getMessage).toList.mkString(","))
      case Failure(e) => interpolation.abort(Literal(0, cql), 0, e.getMessage)
    }

  }

  def evaluate(interpolation: RuntimeInterpolation): (String, List[SerializableValueBy[Int]]) =
    interpolation.parts.foldLeft(("", List.empty[SerializableValueBy[Int]])) {
      case ((cql, values), Literal(_, string)) =>
        (cql + string, values)
      case ((cql, values), Substitution(index, value)) =>
        (cql + "?", values :+ SerializableValueBy(index, value))
    }

  private[this] def parseStatement(cql: String): Try[Statements] = {

    def parseStatementWith[T](
        parseFunction: (String) => CqlParser.ParseResult[T],
        mapResult: T => Statements): Try[Statements] =
      parseFunction(cql) match {
        case CqlParser.Success(result, _) => Success(mapResult(result))
        case CqlParser.Failure(msg, _)    => Failure(new IllegalArgumentException(msg))
        case CqlParser.Error(msg, _)      => Failure(new IllegalArgumentException(msg))
      }

    def parseDataManipulationStatement(cql: String): Try[Statements] =
      parseStatementWith[DataManipulation](
        parseFunction = CqlParser.parseDML,
        mapResult = dm => DML(dm))

    def parseDataDefinitionStatement(cql: String): Try[Statements] =
      parseStatementWith[Seq[DataDefinition]](
        parseFunction = CqlParser.parseSchema,
        mapResult = seq => DDL(seq))

    parseDataManipulationStatement(cql).recoverWith {
      case NonFatal(e1) =>
        parseDataDefinitionStatement(cql).recoverWith {
          case NonFatal(e2) =>
            Failure(ParseError(e1.getMessage :: e2.getMessage :: Nil))
        }
    }
  }
}
