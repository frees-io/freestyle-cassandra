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

import cats.implicits._
import cats.~>
import com.datastax.driver.core._
import freestyle._
import freestyle.FreeS
import freestyle.cassandra.schema.provider.metadata.SchemaConversions
import freestyle.cassandra.schema.{SchemaDefinition, SchemaDefinitionProviderError}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

class MetadataSchemaProvider(cluster: Cluster)
    extends SchemaDefinitionProvider
    with SchemaConversions {

  def extractTables(keyspaceMetadata: KeyspaceMetadata): List[AbstractTableMetadata] =
    keyspaceMetadata.getTables.asScala.toList

  def extractIndexes(tableMetadataList: List[AbstractTableMetadata]): List[IndexMetadata] =
    tableMetadataList.flatMap {
      case (t: TableMetadata) => t.getIndexes.asScala.toList
      case _                  => Nil
    }

  def extractUserTypes(keyspaceMetadata: KeyspaceMetadata): List[UserType] =
    keyspaceMetadata.getUserTypes.asScala.toList

  override def schemaDefinition: Either[SchemaDefinitionProviderError, SchemaDefinition] = {

    import scala.concurrent.ExecutionContext.Implicits.global

    import freestyle.async.implicits._
    import freestyle.cassandra.api._
    import freestyle.cassandra.handlers.implicits._

    implicit val clusterAPIInterpreter: ClusterAPI.Op ~> Future =
      clusterAPIHandler[Future] andThen apiInterpreter[Future, Cluster](cluster)

    def metadataF[F[_]](implicit clusterAPI: ClusterAPI[F]): FreeS[F, Metadata] =
      for {
        _        <- clusterAPI.connect
        metadata <- clusterAPI.metadata
        _        <- clusterAPI.close
      } yield metadata

    Either.catchNonFatal {
      Await.result(metadataF[ClusterAPI.Op].interpret[Future], 10.seconds)
    } leftMap (SchemaDefinitionProviderError(_)) flatMap { metadata =>
      val keyspaceList: List[KeyspaceMetadata]   = metadata.getKeyspaces.asScala.toList
      val tableList: List[AbstractTableMetadata] = keyspaceList.flatMap(extractTables)
      val indexList: List[IndexMetadata]         = extractIndexes(tableList)
      val userTypeList: List[UserType]           = keyspaceList.flatMap(extractUserTypes)

      for {
        keyspaces <- keyspaceList.traverse(toCreateKeyspace)
        tables    <- tableList.traverse(toCreateTable)
        indexes   <- indexList.traverse(toCreateIndex(_))
        userTypes <- userTypeList.traverse(toUserType)
      } yield keyspaces ++ tables ++ indexes ++ userTypes

    }
  }
}

object MetadataSchemaProvider {

  implicit def metadataSchemaProvider(implicit cluster: Cluster): SchemaDefinitionProvider =
    new MetadataSchemaProvider(cluster)

}
