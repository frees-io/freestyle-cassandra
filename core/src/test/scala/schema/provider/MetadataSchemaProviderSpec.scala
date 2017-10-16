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

import java.io.{ByteArrayInputStream, InputStream}

import com.datastax.driver.core._
import freestyle.cassandra.config.TestDecoderUtils
import freestyle.cassandra.schema.SchemaDefinition
import org.scalacheck.Prop._
import troy.cql.ast.TableName

import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}

class MetadataSchemaProviderSpec extends TestDecoderUtils {

  import cats.instances.future._
  import freestyle.cassandra.schema.MetadataArbitraries._

  import scala.concurrent.ExecutionContext.Implicits.global

  "schemaDefinition" should {

    "return the right schema definition for valid values" in {
      check {
        forAll(schemaGen) {
          case (keyspace, tables, indexes, userTypes) =>
            implicit val clusterMock: Cluster = mock[ClusterTest]
            val sessionMock: Session          = mock[Session]
            val metadataMock: Metadata        = mock[MetadataTest]
            (clusterMock.connect _: () => Session).expects().returns(sessionMock)
            (clusterMock.getMetadata _).expects().returns(metadataMock)
            (metadataMock.getKeyspaces _).expects().returns(List(keyspace.keyspaceMetadata).asJava)

            val indexedWithTableName = indexes.map { genIndex =>
              val createIndex =
                genIndex.createIndex.copy(tableName = tables.head.createTable.tableName)
              genIndex.copy(createIndex = createIndex)
            }

            val clusterFuture: Future[Cluster] = Future.successful(clusterMock)

            val metadataSchemaProvider = new MetadataSchemaProvider[Future](clusterFuture) {

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

            Await.result(
              metadataSchemaProvider.schemaDefinition,
              scala.concurrent.duration.Duration.Inf) isEqualTo expected
        }
      }
    }

    "return a left if there is an error fetching the metadata from cluster" in {
      implicit val clusterMock: Cluster = mock[ClusterTest]
      val sessionMock: Session          = mock[Session]
      val exception: Throwable          = new RuntimeException("Test exception")
      (clusterMock.connect _: () => Session).expects().returns(sessionMock)
      (clusterMock.getMetadata _).expects().throws(exception)

      Await.result(MetadataSchemaProvider.metadataSchemaProvider[Future].schemaDefinition.recover {
        case _ => Seq.empty
      }, scala.concurrent.duration.Duration.Inf) shouldBe Seq.empty
    }

    "clusterProvider" should {

      "return an error if the cluster configuration is not valid" in {
        val is: InputStream = new ByteArrayInputStream("cluster = {}".getBytes)

        val clusterProvider = MetadataSchemaProvider.clusterProvider[Future](is)

        Await.result(clusterProvider.recover {
          case _ => Seq.empty
        }, scala.concurrent.duration.Duration.Inf) shouldBe Seq.empty
      }

      "return the valid configuration" in {
        val is: InputStream =
          new ByteArrayInputStream(s"cluster = ${validClusterConfiguration.print}".getBytes)

        val cluster = Await.result(
          MetadataSchemaProvider.clusterProvider[Future](is),
          scala.concurrent.duration.Duration.Inf)

        Option(cluster.getClusterName) shouldBe validClusterConfiguration.name
      }

    }

    "create a metadataSchemaProvider from a Reader with the configuration" in {
      val is: InputStream = new ByteArrayInputStream("cluster = {}".getBytes)

      type SchemaProviderFuture = SchemaDefinitionProvider[Future]

      MetadataSchemaProvider.metadataSchemaProvider[Future](Future.successful(is)) shouldBe a[
        SchemaProviderFuture]
    }
  }

}
