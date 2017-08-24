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

import com.datastax.driver.core._
import com.google.common.util.concurrent.ListenableFuture
import freestyle.cassandra.TestUtils.{successfulFuture, MatchersUtil}
import freestyle.cassandra.schema.SchemaDefinition
import org.scalacheck.Prop._
import org.scalamock.scalatest.MockFactory
import org.scalatest.WordSpec
import org.scalatest.prop.Checkers
import troy.cql.ast.TableName

import scala.collection.JavaConverters._

class MetadataSchemaProviderSpec
    extends WordSpec
    with MatchersUtil
    with Checkers
    with MockFactory {

  import freestyle.cassandra.schema.MetadataArbitraries._

  "schemaDefinition" should {

    "return the right schema definition for valid values" in {
      check {
        forAll(schemaGen) {
          case (keyspace, tables, indexes, userTypes) =>
            val clusterMock: Cluster              = mock[ClusterTest]
            val sessionMock: Session              = mock[Session]
            val metadataMock: Metadata            = mock[MetadataTest]
            val result: ListenableFuture[Session] = successfulFuture(sessionMock)
            (clusterMock.connectAsync _: () => ListenableFuture[Session]).expects().returns(result)
            (clusterMock.getMetadata _).expects().returns(metadataMock)
            (metadataMock.getKeyspaces _).expects().returns(List(keyspace.keyspaceMetadata).asJava)
            (clusterMock.closeAsync _).expects().returns(CloseFutureTest)

            val indexedWithTableName = indexes.map { genIndex =>
              val createIndex =
                genIndex.createIndex.copy(tableName = tables.head.createTable.tableName)
              genIndex.copy(createIndex = createIndex)
            }

            val metadataSchemaProvider = new MetadataSchemaProvider(clusterMock) {

              override def readTable(metadata: IndexMetadata): TableName =
                tables.head.createTable.tableName

              override def extractTables(
                  keyspaceMetadata: KeyspaceMetadata): List[AbstractTableMetadata] =
                tables.toList.map(_.tableMetadata)

              override def extractIndexes(
                  tableMetadataList: List[AbstractTableMetadata]): List[IndexMetadata] =
                indexedWithTableName.map(_.indexMetadata)

              override def extractUserTypes(keyspaceMetadata: KeyspaceMetadata): List[UserType] =
                userTypes.map(_.userType)
            }

            val expected: SchemaDefinition = Seq(keyspace.createKeyspace) ++
              tables.toList.map(_.createTable) ++
              indexedWithTableName.map(_.createIndex) ++
              userTypes.map(_.createType)

            metadataSchemaProvider.schemaDefinition isEqualTo Right(expected)
        }
      }
    }

    "return a left if there is an error fetching the metadata from cluster" in {
      implicit val clusterMock: Cluster     = mock[ClusterTest]
      val sessionMock: Session              = mock[Session]
      val result: ListenableFuture[Session] = successfulFuture(sessionMock)
      val exception: Throwable              = new RuntimeException("Test exception")
      (clusterMock.connectAsync _: () => ListenableFuture[Session]).expects().returns(result)
      (clusterMock.getMetadata _).expects().throws(exception)

      MetadataSchemaProvider.metadataSchemaProvider.schemaDefinition.isLeft
    }

  }

}
