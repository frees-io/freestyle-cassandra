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
package query

trait StatementGenerator[A] {
  def select(table: String): String
  def insert(table: String): String
}

object StatementGenerator {
  def apply[A](implicit ev: StatementGenerator[A]): StatementGenerator[A] = ev

  implicit def genericGenerator[A](implicit fieldLister: FieldLister[A]): StatementGenerator[A] =
    new StatementGenerator[A] {
      override def select(table: String): String = {
        val fields = fieldLister.list.mkString(",")
        s"SELECT $fields FROM $table"
      }

      override def insert(table: String): String = {
        val fieldNames   = fieldLister.list
        val fields       = fieldNames.mkString(",")
        val placeholders = List.fill(fieldNames.size)("?").mkString(",")
        s"INSERT INTO $table ($fields) VALUES ($placeholders)"
      }
    }
}
