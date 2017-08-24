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
import com.datastax.driver.core.{
  AbstractTableMetadata,
  ColumnMetadata,
  IndexMetadata,
  KeyspaceMetadata,
  TupleType,
  UserType,
  DataType => DatastaxDataType
}
import freestyle.cassandra.schema.{SchemaDefinitionProviderError, SchemaResult}
import troy.cql.ast._
import troy.cql.ast.ddl.Keyspace.Replication
import troy.cql.ast.ddl.Table.PrimaryKey
import troy.cql.ast.ddl.{Field, Index, Table}

import scala.collection.JavaConverters._
import scala.language.postfixOps

trait SchemaConversions {

  def toCreateKeyspace(keyspaceMetadata: KeyspaceMetadata): SchemaResult[CreateKeyspace] =
    Either.catchNonFatal {
      val name: String = Option(keyspaceMetadata.getName)
        .getOrElse(throw new NullPointerException("Schema name is null"))
      val replication: Option[Replication] = Option(keyspaceMetadata.getReplication)
        .flatMap { m =>
          val seq = m.asScala.toSeq
          if (seq.isEmpty) None else Option(Replication(seq.sortBy(_._1)))
        }
      CreateKeyspace(
        ifNotExists = false,
        keyspaceName = KeyspaceName(name),
        properties = replication map (Seq(_)) getOrElse Seq.empty)
    } leftMap (SchemaDefinitionProviderError(_))

  def toCreateTable(metadata: AbstractTableMetadata): SchemaResult[CreateTable] =
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

  def readTable(metadata: IndexMetadata): TableName =
    TableName(Some(KeyspaceName(metadata.getTable.getKeyspace.getName)), metadata.getTable.getName)

  def toCreateIndex(
      metadata: IndexMetadata,
      readTable: (IndexMetadata) => TableName = readTable): Either[
    SchemaDefinitionProviderError,
    CreateIndex] =
    Either.catchNonFatal {
      CreateIndex(
        isCustom = metadata.isCustomIndex,
        ifNotExists = false,
        indexName = Option(metadata.getName),
        tableName = readTable(metadata),
        identifier = Index.Identifier(metadata.getTarget),
        using =
          if (metadata.isCustomIndex)
            // The options are not visible in the IndexMetadata class
            Some(Index.Using(metadata.getIndexClassName, None))
          else None
      )
    } leftMap (SchemaDefinitionProviderError(_))

  def toUserType(userType: UserType): SchemaResult[CreateType] =
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
      datastaxDataType: DatastaxDataType): SchemaResult[Field] =
    toDataType(datastaxDataType) map { dataType =>
      Field(name, dataType)
    }

  private[this] def toTableColumn(metadata: ColumnMetadata): SchemaResult[Table.Column] =
    toDataType(metadata.getType).map { dataType =>
      Table.Column(
        name = metadata.getName,
        dataType = dataType,
        isStatic = metadata.isStatic,
        isPrimaryKey = false)
    }

  private[this] def toDataType(dataType: DatastaxDataType): SchemaResult[DataType] = {

    import DatastaxDataType._

    def toDataTypeNative(dataType: DatastaxDataType): SchemaResult[DataType.Native] =
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
          Left(SchemaDefinitionProviderError(s"Native DataType ${dataType.getName} not supported"))
      }

    def toCollectionType(collectionType: CollectionType): SchemaResult[DataType] = {

      val typeArgs: List[DatastaxDataType] = collectionType.getTypeArguments.asScala.toList

      val maybeCol = collectionType.getName match {
        case Name.LIST =>
          typeArgs.headOption map { typeArg =>
            toDataTypeNative(typeArg) map DataType.List
          }
        case Name.SET =>
          typeArgs.headOption map { typeArg =>
            toDataTypeNative(typeArg) map DataType.Set
          }
        case Name.MAP =>
          for {
            t1 <- typeArgs.headOption
            t2 <- typeArgs.tail.headOption
          } yield (toDataTypeNative(t1) |@| toDataTypeNative(t2)).map(DataType.Map)
        case _ => None
      }

      maybeCol getOrElse {
        Left(
          SchemaDefinitionProviderError(
            s"Error parsing collection DataType '${collectionType.asFunctionParameterString()}'"))
      }
    }

    def toTupleType(tupleType: TupleType): SchemaResult[DataType] =
      tupleType.getComponentTypes.asScala.toList traverse toDataTypeNative map DataType.Tuple

    dataType match {
      case nativeType: NativeType         => toDataTypeNative(nativeType)
      case customType: CustomType         => Right(DataType.Custom(customType.getCustomTypeClassName))
      case collectionType: CollectionType => toCollectionType(collectionType)
      case tupleType: TupleType           => toTupleType(tupleType)
      case userType: UserType =>
        Right(DataType.UserDefined(KeyspaceName(userType.getKeyspace), userType.getTypeName))
    }
  }

  private[this] def toPrimaryKey(
      partitionKeys: List[ColumnMetadata],
      clusteringColumns: List[ColumnMetadata]): SchemaResult[PrimaryKey] =
    PrimaryKey(partitionKeys.map(_.getName), clusteringColumns.map(_.getName)).asRight

}
