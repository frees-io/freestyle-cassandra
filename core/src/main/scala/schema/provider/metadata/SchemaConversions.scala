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
package schema.provider.metadata

import cats.implicits._
import cats.{~>, MonadError}
import com.datastax.driver.core.{
  Cluster,
  ColumnMetadata,
  IndexMetadata,
  KeyspaceMetadata,
  Metadata,
  TableMetadata,
  TupleType,
  UserType,
  DataType => DatastaxDataType
}
import freestyle._
import freestyle.FreeS
import freestyle.cassandra.schema.{SchemaDefinition, SchemaDefinitionProviderError}
import troy.cql.ast._
import troy.cql.ast.ddl.Keyspace.Replication
import troy.cql.ast.ddl.{Field, Index, Table}
import troy.cql.ast.ddl.Table.PrimaryKey

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

trait SchemaConversions {

  def toCreateKeyspace(
      keyspaceMetadata: KeyspaceMetadata): Either[SchemaDefinitionProviderError, CreateKeyspace] =
    Either.catchNonFatal {
      CreateKeyspace(
        ifNotExists = false,
        keyspaceName = KeyspaceName(keyspaceMetadata.getName),
        properties = Seq(Replication(keyspaceMetadata.getReplication.asScala.toSeq)))
    } leftMap (SchemaDefinitionProviderError(_))

  def toCreateTable(metadata: TableMetadata): Either[SchemaDefinitionProviderError, CreateTable] =
    Either.catchNonFatal {
      for {
        columns <- metadata.getColumns.asScala.toList.traverse(toTableColumn)
        primaryKey <- toPrimaryKey(
          metadata.getPartitionKey.asScala.toList,
          metadata.getClusteringColumns.asScala.toList)
      } yield
        CreateTable(
          ifNotExists = false,
          tableName = TableName(Some(KeyspaceName(metadata.getKeyspace.getName)), metadata.getName),
          columns = columns,
          primaryKey = Some(primaryKey),
          options = Seq.empty
        )
    } leftMap (SchemaDefinitionProviderError(_)) joinRight

  def toCreateIndex(metadata: IndexMetadata): Either[SchemaDefinitionProviderError, CreateIndex] =
    Either.catchNonFatal {
      CreateIndex(
        isCustom = metadata.isCustomIndex,
        ifNotExists = false,
        indexName = Option(metadata.getName),
        tableName = TableName(
          Some(KeyspaceName(metadata.getTable.getKeyspace.getName)),
          metadata.getTable.getName),
        identifier = Index.Identifier(metadata.getTarget),
        using =
          if (metadata.isCustomIndex)
            Some(Index.Using(metadata.getIndexClassName, toUsingOptions(metadata)))
          else None
      )
    } leftMap (SchemaDefinitionProviderError(_))

  def toUserType(userType: UserType): Either[SchemaDefinitionProviderError, CreateType] =
    Either.catchNonFatal {
      userType.getFieldNames.asScala.toList.traverse { fieldName =>
        toField(fieldName, userType.getFieldType(fieldName))
      } map { list =>
        CreateType(
          ifNotExists = false,
          typeName = TypeName(Some(KeyspaceName(userType.getKeyspace)), userType.getTypeName),
          fields = list)
      }
    } leftMap (SchemaDefinitionProviderError(_)) joinRight

  private[this] def toField(
      name: String,
      datastaxDataType: DatastaxDataType): Either[SchemaDefinitionProviderError, Field] =
    toDataType(datastaxDataType) map { dataType =>
      Field(name, dataType)
    }

  private[this] def toTableColumn(
      metadata: ColumnMetadata): Either[SchemaDefinitionProviderError, Table.Column] =
    toDataType(metadata.getType).map { dataType =>
      Table.Column(
        name = metadata.getName,
        dataType = dataType,
        isStatic = metadata.isStatic,
        isPrimaryKey = false)
    }

  private[this] def toDataType(
      dataType: DatastaxDataType): Either[SchemaDefinitionProviderError, DataType] = {

    import DatastaxDataType._

    def toDataTypeNative(
        dataType: DatastaxDataType): Either[SchemaDefinitionProviderError, DataType.Native] =
      dataType.getName match {
        case Name.ASCII     => DataType.Ascii.asRight
        case Name.BIGINT    => DataType.BigInt.asRight
        case Name.BLOB      => DataType.Blob.asRight
        case Name.BOOLEAN   => DataType.Boolean.asRight
        case Name.COUNTER   => DataType.Counter.asRight
        case Name.DATE      => DataType.Date.asRight
        case Name.DECIMAL   => DataType.Decimal.asRight
        case Name.DOUBLE    => DataType.Double.asRight
        case Name.FLOAT     => DataType.Float.asRight
        case Name.INET      => DataType.Inet.asRight
        case Name.INT       => DataType.Int.asRight
        case Name.SMALLINT  => DataType.Smallint.asRight
        case Name.TEXT      => DataType.Text.asRight
        case Name.TIME      => DataType.Time.asRight
        case Name.TIMESTAMP => DataType.Timestamp.asRight
        case Name.TIMEUUID  => DataType.Timeuuid.asRight
        case Name.TINYINT   => DataType.Tinyint.asRight
        case Name.UUID      => DataType.Uuid.asRight
        case Name.VARCHAR   => DataType.Varchar.asRight
        case Name.VARINT    => DataType.Varint.asRight
        case _ =>
          Left(SchemaDefinitionProviderError(s"DataType ${dataType.getName} not supported"))
      }

    def toCollectionType(
        collectionType: CollectionType): Either[SchemaDefinitionProviderError, DataType] = {

      val typeArgs: List[DatastaxDataType] = collectionType.getTypeArguments.asScala.toList

      val maybeCol = collectionType.getName match {
        case Name.LIST | Name.SET =>
          typeArgs.headOption map { typeArg =>
            toDataTypeNative(typeArg) map DataType.List
          }
        case Name.MAP =>
          typeArgs.headOption.flatMap(t1 => typeArgs.tail.headOption.map(t2 => (t1, t2))) map {
            tupleArgs =>
              for {
                dataNative1 <- toDataTypeNative(tupleArgs._1)
                dataNative2 <- toDataTypeNative(tupleArgs._2)
              } yield DataType.Map(dataNative1, dataNative2)
          }
        case _ => None
      }

      maybeCol getOrElse {
        Left(
          SchemaDefinitionProviderError(
            s"Error parsing collection DataType '${collectionType.asFunctionParameterString()}'"))
      }
    }

    def toTupleType(tupleType: TupleType): Either[SchemaDefinitionProviderError, DataType] =
      tupleType.getComponentTypes.asScala.toList traverse toDataTypeNative map DataType.Tuple

    dataType match {
      case nativeType: NativeType =>
        toDataTypeNative(nativeType) map (Right(_)) getOrElse {
          Left(SchemaDefinitionProviderError(s"DataType ${dataType.getName} not supported"))
        }
      case customType: CustomType         => Right(DataType.Custom(customType.getCustomTypeClassName))
      case collectionType: CollectionType => toCollectionType(collectionType)
      case tupleType: TupleType           => toTupleType(tupleType)
      case userType: UserType =>
        DataType.UserDefined(KeyspaceName(userType.getKeyspace), userType.getTypeName)
    }

    toDataTypeNative(dataType) map (Right(_)) getOrElse {
      dataType match {
        case customType: CustomType     => Right(DataType.Custom(customType.getCustomTypeClassName))
        case collection: CollectionType => toCollectionType(collection)
        case _ =>
          Left(SchemaDefinitionProviderError(s"DataType ${dataType.getName} not supported"))
      }
    }
  }

  private[this] def toPrimaryKey(
      partitionKeys: List[ColumnMetadata],
      clusteringColumns: List[ColumnMetadata]): Either[SchemaDefinitionProviderError, PrimaryKey] =
    PrimaryKey(partitionKeys.map(_.getName), clusteringColumns.map(_.getName)).asRight

  private[this] def toUsingOptions(metadata: IndexMetadata): Option[MapLiteral] = {

    def toTerm(key: String): Option[(Term, Term)] =
      Option(metadata.getOption(key)) map (value => (Constant(key), Constant(value)))

    val terms = toTerm(IndexMetadata.INDEX_KEYS_OPTION_NAME).toSeq ++
      toTerm(IndexMetadata.INDEX_ENTRIES_OPTION_NAME).toSeq

    if (terms.nonEmpty) Some(MapLiteral(terms)) else None
  }

}
