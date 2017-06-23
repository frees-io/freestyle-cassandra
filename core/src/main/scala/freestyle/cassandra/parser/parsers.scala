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

package freestyle.cassandra.parser

import cats.syntax.either._
import freestyle.cassandra.parser.common._
import io.circe.parser.{parse => circeParse}

import scala.util.parsing.combinator._

object parsers extends RegexParsers {

  import decoders._
  import definitions._
  import model._

  type ParseResult[T] = Either[String, T]

  final class ParseResultOps[T](parseResult: ParseResult[T]) {

    def toParser: Parser[T] = parseResult match {
      case Right(t) => success(t)
      case Left(m)  => err(m)
    }

  }

  implicit def parseResultOps[T](parseResult: ParseResult[T]): ParseResultOps[T] =
    new ParseResultOps(parseResult)

  def keyspaceParser: Parser[Keyspace] =
    "CREATE KEYSPACE" ~ "IF NOT EXISTS".? ~
      (QuotedNameRegex | UnquotedNameRegex) ~
      ((WithRepRegex ~ AndDWRegex) |
        (WithDWRegex ~ AndRepRegex)) >> {
      case _ ~ _ ~ NameRegex(keyspaceName) ~ (WithRepRegex(json) ~ AndDWRegex(_, dw)) =>
        parseKeyspace(keyspaceName, json, dw).toParser
      case _ ~ _ ~ NameRegex(keyspaceName) ~ (WithDWRegex(dw) ~ AndRepRegex(json)) =>
        parseKeyspace(keyspaceName, json, dw).toParser
    }

  private[this] def parseReplication(jsonString: String): ParseResult[KeyspaceReplication] =
    circeParse(jsonString.replaceAllLiterally("'", "\""))
      .flatMap(_.as[KeyspaceReplication])
      .leftMap(_.getMessage())

  private[this] def parseDurableWrites(value: String): ParseResult[Option[Boolean]] =
    Either.catchNonFatal(Option(value).map(_.toBoolean)).leftMap { _ =>
      s"'$value' is not a valid durable_writes value, boolean expected"
    }

  private[this] def parseKeyspace(
      keyspaceName: String,
      jsonReplication: String,
      durableWrites: String): ParseResult[Keyspace] = {
    for {
      replication   <- parseReplication(jsonReplication)
      durableWrites <- parseDurableWrites(durableWrites)
    } yield Keyspace(keyspaceName, replication, durableWrites)
  }
}
