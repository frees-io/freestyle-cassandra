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

import java.util.concurrent.{Callable, Executors}

import com.google.common.util.concurrent.{ListenableFuture, ListeningExecutorService, MoreExecutors}
import org.scalatest.Matchers

object TestUtils {

  val service: ListeningExecutorService =
    MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(10))

  def successfulFuture[T](value: T): ListenableFuture[T] =
    service.submit(new Callable[T] {
      override def call(): T = value
    })

  val exception: Throwable = new RuntimeException("Test exception")

  def failedFuture[T]: ListenableFuture[T] =
    service.submit(new Callable[T] {
      override def call(): T = throw exception
    })

  class Null[A] { var t: A = _ }

  object Null {
    def apply[A]: A = new Null[A].t
  }

  trait MatchersUtil extends Matchers {

    final class AnyOps[T](value: T) {

      def isEqualTo(other: T): Boolean = {
        value shouldBe other
        value == other
      }

      def isLikeTo(f: T => Boolean): Boolean =
        f(value)

    }

    implicit def anyOps[T](value: T): AnyOps[T] = new AnyOps[T](value)

  }

  type EitherM[T] = Either[Throwable, T]

  val reservedKeywords = List(
    "ADD",
    "AGGREGATE",
    "ALL",
    "ALLOW",
    "ALTER",
    "AND",
    "ANY",
    "APPLY",
    "AS",
    "ASC",
    "ASCII",
    "AUTHORIZE",
    "BATCH",
    "BEGIN",
    "BIGINT",
    "BLOB",
    "BOOLEAN",
    "BY",
    "CLUSTERING",
    "COLUMNFAMILY",
    "COMPACT",
    "CONSISTENCY",
    "COUNT",
    "COUNTER",
    "CREATE",
    "CUSTOM",
    "DECIMAL",
    "DELETE",
    "DESC",
    "DISTINCT",
    "DOUBLE",
    "DROP",
    "EACH_QUORUM",
    "ENTRIES",
    "EXISTS",
    "FILTERING",
    "FLOAT",
    "FROM",
    "FROZEN",
    "FULL",
    "GRANT",
    "IF",
    "IN",
    "INDEX",
    "INET",
    "INFINITY",
    "INSERT",
    "INT",
    "INTO",
    "KEY",
    "KEYSPACE",
    "KEYSPACES",
    "LEVEL",
    "LIMIT",
    "LIST",
    "LOCAL_ONE",
    "LOCAL_QUORUM",
    "MAP",
    "MATERIALIZED",
    "MODIFY",
    "NAN",
    "NORECURSIVE",
    "NOSUPERUSER",
    "NOT",
    "OF",
    "ON",
    "ONE",
    "ORDER",
    "PARTITION",
    "PASSWORD",
    "PER",
    "PERMISSION",
    "PERMISSIONS",
    "PRIMARY",
    "QUORUM",
    "RENAME",
    "REVOKE",
    "SCHEMA",
    "SELECT",
    "SET",
    "STATIC",
    "STORAGE",
    "SUPERUSER",
    "TABLE",
    "TEXT",
    "TIME",
    "TIMESTAMP",
    "TIMEUUID",
    "THREE",
    "TO",
    "TOKEN",
    "TRUNCATE",
    "TTL",
    "TUPLE",
    "TWO",
    "TYPE",
    "UNLOGGED",
    "UPDATE",
    "USE",
    "USER",
    "USERS",
    "USING",
    "UUID",
    "VALUES",
    "VARCHAR",
    "VARINT",
    "VIEW",
    "WHERE",
    "WITH",
    "WRITETIME"
  )

}
