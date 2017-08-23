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

import java.io.InputStream

import com.datastax.driver.core.DataType.Name
import com.datastax.driver.core.{
  CodecRegistry,
  ProtocolVersion,
  TupleType,
  UserType,
  UserTypeFieldTest,
  DataType => DatastaxDataType
}
import freestyle.cassandra.TestUtils.Null
import freestyle.cassandra.config.model.Credentials
import freestyle.cassandra.schema.provider.SchemaDefinitionProvider
import troy.cql.ast._
import troy.cql.ast.ddl.{Field, Keyspace, Table}
import troy.cql.ast.dml.{Operator, Select, WhereClause}

object SchemaData {

  val keyspaceName: String = "test"
  val keyspaceProperties: Seq[(String, String)] =
    Seq(("class", "SimpleStrategy"), ("replication_factor", "1"))

  val userTypeName: String = "customUserType"

  val CQLInputStream: InputStream = SchemaData.getClass.getResourceAsStream("/sampleSchema.sql")

  val keyspaceDef: CreateKeyspace = CreateKeyspace(
    ifNotExists = false,
    keyspaceName = KeyspaceName(keyspaceName),
    properties = Seq(Keyspace.Replication(keyspaceProperties))
  )

  val tableDef: CreateTable = CreateTable(
    ifNotExists = true,
    tableName = TableName(Some(KeyspaceName("test")), "posts"),
    columns = Seq(
      Table.Column(
        name = "author_id",
        dataType = DataType.Text,
        isStatic = false,
        isPrimaryKey = false),
      Table.Column(
        name = "post_id",
        dataType = DataType.Timeuuid,
        isStatic = false,
        isPrimaryKey = false),
      Table.Column(
        name = "post_title",
        dataType = DataType.Text,
        isStatic = false,
        isPrimaryKey = false)
    ),
    primaryKey = Some(Table.PrimaryKey(Seq("author_id"), Seq("post_id"))),
    options = Seq.empty
  )

  val selectStatementCQL: String =
    """
      |SELECT post_title
      |FROM test.posts
      |WHERE author_id = '1';
    """.stripMargin

  val selectStatement: SelectStatement = SelectStatement(
    mod = None,
    selection = Select.SelectClause(
      Seq(Select.SelectionClauseItem(selector = Select.ColumnName("post_title"), as = None))),
    from = tableDef.tableName,
    where = Some(
      WhereClause(Seq(WhereClause.Relation
        .Simple(columnName = "author_id", operator = Operator.Equals, term = Constant("1"))))),
    orderBy = None,
    perPartitionLimit = None,
    limit = None,
    allowFiltering = false
  )

  val invalidSelectStatementCQL: String =
    """
      |SELECT unknown_column
      |FROM test.posts
      |WHERE author_id = '1';
    """.stripMargin

  val invalidSelectStatement: SelectStatement = selectStatement.copy(
    selection = Select.SelectClause(
      Seq(Select.SelectionClauseItem(selector = Select.ColumnName("unknown_column"), as = None))))

  val schemaDefinitionProvider = new SchemaDefinitionProvider {
    override def schemaDefinition: Either[SchemaDefinitionProviderError, SchemaDefinition] =
      Right(Seq(keyspaceDef, tableDef))
  }

}
