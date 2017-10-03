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

import java.io.Reader

import cats.MonadError
import cats.implicits._
import com.datastax.driver.core._
import freestyle.async.AsyncContext
import freestyle.cassandra.api.{apiInterpreter, ClusterAPI}
import freestyle.cassandra.config.Decoders
import freestyle.cassandra.handlers.implicits.clusterAPIHandler
import freestyle.cassandra.schema.SchemaDefinition
import freestyle.cassandra.schema.provider.metadata.SchemaConversions
import freestyle.{FreeS, _}

import scala.collection.JavaConverters._
import scala.language.postfixOps

class MetadataSchemaProvider[M[_]](
    val clusterProvider: M[Cluster])(implicit AC: AsyncContext[M], API: ClusterAPI[ClusterAPI.Op])
    extends SchemaDefinitionProvider[M]
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

  override def schemaDefinition(implicit E: MonadError[M, Throwable]): M[SchemaDefinition] = {

    def guarantee[F[_], A](fa: F[A], finalizer: F[Unit])(
        implicit E: MonadError[F, Throwable]): F[A] =
      E.flatMap(E.attempt(fa)) { e =>
        E.flatMap(finalizer)(_ => e.fold(E.raiseError, E.pure))
      }

    def metadataF: FreeS[ClusterAPI.Op, Metadata] = API.connect *> API.metadata

    def closeF: FreeS[ClusterAPI.Op, Unit] = API.close

    E.flatMap(clusterProvider) { cluster =>
      implicit val H: FSHandler[ClusterAPI.Op, M] =
        clusterAPIHandler[M] andThen apiInterpreter[M, Cluster](cluster)

      E.flatMap(guarantee(metadataF.interpret[M], closeF.interpret[M])) { metadata =>
        val keyspaceList: List[KeyspaceMetadata]   = metadata.getKeyspaces.asScala.toList
        val tableList: List[AbstractTableMetadata] = keyspaceList.flatMap(extractTables)
        val indexList: List[IndexMetadata]         = extractIndexes(tableList)
        val userTypeList: List[UserType]           = keyspaceList.flatMap(extractUserTypes)

        E.map4(
          keyspaceList.traverse(toCreateKeyspace[M]),
          tableList.traverse(toCreateTable[M]),
          indexList.traverse(toCreateIndex[M](_)),
          userTypeList.traverse(toUserType[M])
        )(_ ++ _ ++ _ ++ _)
      }
    }
  }
}

object MetadataSchemaProvider {

  implicit def metadataSchemaProvider[M[_]](
      implicit cluster: Cluster,
      AC: AsyncContext[M],
      E: MonadError[M, Throwable],
      API: ClusterAPI[ClusterAPI.Op]): SchemaDefinitionProvider[M] =
    new MetadataSchemaProvider[M](E.pure(cluster))

  def clusterProvider[M[_]](configReader: Reader)(
      implicit E: MonadError[M, Throwable]): M[Cluster] = {
    import classy.config._
    import classy.{DecodeError, Decoder}
    import com.datastax.driver.core.Cluster
    import com.typesafe.config.{Config, ConfigFactory}

    def decodeConfig: M[Either[DecodeError, Cluster]] =
      E.catchNonFatal {
        val decoders: Decoders[Config]                = new Decoders[Config]
        val decoder: Decoder[Config, Cluster.Builder] = readConfig[Config]("cluster") andThen decoders.clusterBuilderDecoder
        decoder(ConfigFactory.parseReader(configReader)).map(_.build())
      }

    E.flatMap(decodeConfig) {
      case Right(cluster) => E.pure(cluster)
      case Left(error)    => E.raiseError(new IllegalArgumentException(error.toPrettyString))
    }
  }

  def metadataSchemaProvider[M[_]](configReader: Reader)(
      implicit AC: AsyncContext[M],
      E: MonadError[M, Throwable],
      API: ClusterAPI[ClusterAPI.Op]): SchemaDefinitionProvider[M] =
    new MetadataSchemaProvider[M](clusterProvider[M](configReader))

}
