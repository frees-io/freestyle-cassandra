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

import cats.MonadError
import cats.instances.list._
import cats.syntax.traverse._
import com.datastax.driver.core.{
  AbstractTableMetadata,
  ColumnMetadata,
  IndexMetadata,
  KeyspaceMetadata,
  TupleType,
  UserType,
  DataType => DatastaxDataType
}
import freestyle.cassandra.schema._
import troy.cql.ast._
import troy.cql.ast.ddl.Keyspace.Replication
import troy.cql.ast.ddl.Table.PrimaryKey
import troy.cql.ast.ddl.{Field, Index, Table}

import scala.collection.JavaConverters._
import scala.language.postfixOps

trait SchemaConversions {

  def toCreateKeyspace[M[_]](keyspaceMetadata: KeyspaceMetadata)(
      implicit M: MonadError[M, Throwable]): M[CreateKeyspace] =
    catchNonFatalAsSchemaError {
      val name: String = Option(keyspaceMetadata.getName)
        .getOrElse(throw new IllegalArgumentException("Schema name is null"))
      val replication: Option[Replication] = Option(keyspaceMetadata.getReplication)
        .flatMap { m =>
          val seq = m.asScala.toSeq
          if (seq.isEmpty) None else Option(Replication(seq.sortBy(_._1)))
        }
      CreateKeyspace(
        ifNotExists = false,
        keyspaceName = KeyspaceName(name),
        properties = replication map (Seq(_)) getOrElse Seq.empty)
    }

  def toCreateTable[M[_]](metadata: AbstractTableMetadata)(
      implicit M: MonadError[M, Throwable]): M[CreateTable] =
    M.flatten {
      catchNonFatalAsSchemaError {
        val columnsM: M[List[Table.Column]] =
          M.traverse(metadata.getColumns.asScala.toList)(toTableColumn[M](_)(M))
        val pKeyM: M[PrimaryKey] = toPrimaryKey(
          metadata.getPartitionKey.asScala.toList,
          metadata.getClusteringColumns.asScala.toList)

        M.map2(columnsM, pKeyM) { (columns, pKey) =>
          CreateTable(
            ifNotExists = false,
            tableName =
              TableName(Some(KeyspaceName(metadata.getKeyspace.getName)), metadata.getName),
            columns = columns,
            primaryKey = Some(pKey),
            options = Seq.empty
          )
        }
      }
    }

  def readTable(metadata: IndexMetadata): TableName =
    TableName(Some(KeyspaceName(metadata.getTable.getKeyspace.getName)), metadata.getTable.getName)

  def toCreateIndex[M[_]](
      metadata: IndexMetadata,
      readTable: (IndexMetadata) => TableName = readTable)(
      implicit M: MonadError[M, Throwable]): M[CreateIndex] =
    catchNonFatalAsSchemaError {
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
    }

  def toUserType[M[_]](userType: UserType)(implicit M: MonadError[M, Throwable]): M[CreateType] =
    M.flatten {
      catchNonFatalAsSchemaError {
        val fieldsM: M[List[Field]] =
          userType.getFieldNames.asScala.toList.traverse { fieldName =>
            toField(fieldName, userType.getFieldType(fieldName))
          }

        val typeName = TypeName(Some(KeyspaceName(userType.getKeyspace)), userType.getTypeName)

        M.map(fieldsM) { list =>
          CreateType(ifNotExists = false, typeName = typeName, fields = list)
        }
      }
    }

  private[this] def toField[M[_]](name: String, datastaxDataType: DatastaxDataType)(
      implicit M: MonadError[M, Throwable]): M[Field] =
    M.map(toDataType(datastaxDataType))(Field(name, _))

  private[this] def toTableColumn[M[_]](metadata: ColumnMetadata)(
      implicit M: MonadError[M, Throwable]): M[Table.Column] =
    M.map(toDataType(metadata.getType)) { dataType =>
      Table.Column(
        name = metadata.getName,
        dataType = dataType,
        isStatic = metadata.isStatic,
        isPrimaryKey = false)
    }

  private[this] def toDataType[M[_]](dataType: DatastaxDataType)(
      implicit M: MonadError[M, Throwable]): M[DataType] = {

    import DatastaxDataType._

    def toDataTypeNative(dataType: DatastaxDataType): M[DataType.Native] =
      dataType.getName match {
        case Name.ASCII     => M.pure(DataType.Ascii)
        case Name.BIGINT    => M.pure(DataType.BigInt)
        case Name.BLOB      => M.pure(DataType.Blob)
        case Name.BOOLEAN   => M.pure(DataType.Boolean)
        case Name.COUNTER   => M.pure(DataType.Counter)
        case Name.DATE      => M.pure(DataType.Date)
        case Name.DECIMAL   => M.pure(DataType.Decimal)
        case Name.DOUBLE    => M.pure(DataType.Double)
        case Name.FLOAT     => M.pure(DataType.Float)
        case Name.INET      => M.pure(DataType.Inet)
        case Name.INT       => M.pure(DataType.Int)
        case Name.SMALLINT  => M.pure(DataType.Smallint)
        case Name.TEXT      => M.pure(DataType.Text)
        case Name.TIME      => M.pure(DataType.Time)
        case Name.TIMESTAMP => M.pure(DataType.Timestamp)
        case Name.TIMEUUID  => M.pure(DataType.Timeuuid)
        case Name.TINYINT   => M.pure(DataType.Tinyint)
        case Name.UUID      => M.pure(DataType.Uuid)
        case Name.VARCHAR   => M.pure(DataType.Varchar)
        case Name.VARINT    => M.pure(DataType.Varint)
        case _ =>
          M.raiseError(
            SchemaDefinitionProviderError(s"Native DataType ${dataType.getName} not supported"))
      }

    def toCollectionType(collectionType: CollectionType): M[DataType] = {

      val typeArgs: List[DatastaxDataType] = collectionType.getTypeArguments.asScala.toList

      val maybeCol: Option[M[DataType]] = collectionType.getName match {
        case Name.LIST =>
          typeArgs.headOption map { typeArg =>
            M.map(toDataTypeNative(typeArg))(DataType.List)
          }
        case Name.SET =>
          typeArgs.headOption map { typeArg =>
            M.map(toDataTypeNative(typeArg))(DataType.Set)
          }
        case Name.MAP =>
          for {
            t1 <- typeArgs.headOption
            t2 <- typeArgs.tail.headOption
          } yield
            M.map2(toDataTypeNative(t1), toDataTypeNative(t2))((t1, t2) => DataType.Map(t1, t2))
        case _ => None
      }

      maybeCol getOrElse {
        M.raiseError(
          SchemaDefinitionProviderError(
            s"Error parsing collection DataType '${collectionType.asFunctionParameterString()}'"))
      }
    }

    def toCustomType(className: String): M[DataType] =
      M.pure(DataType.Custom(className))

    def toUserDefinedType(keyspace: String, typeName: String): M[DataType] =
      M.pure(DataType.UserDefined(KeyspaceName(keyspace), typeName))

    def toTupleType(tupleType: TupleType): M[DataType] =
      M.map(tupleType.getComponentTypes.asScala.toList.traverse(toDataTypeNative))(DataType.Tuple)

    dataType match {
      case nativeType: NativeType =>
        M.widen[DataType.Native, DataType](toDataTypeNative(nativeType))
      case customType: CustomType         => toCustomType(customType.getCustomTypeClassName)
      case collectionType: CollectionType => toCollectionType(collectionType)
      case tupleType: TupleType           => toTupleType(tupleType)
      case userType: UserType             => toUserDefinedType(userType.getKeyspace, userType.getTypeName)
    }
  }

  private[this] def toPrimaryKey[M[_]](
      partitionKeys: List[ColumnMetadata],
      clusteringColumns: List[ColumnMetadata])(
      implicit M: MonadError[M, Throwable]): M[PrimaryKey] =
    M.pure(PrimaryKey(partitionKeys.map(_.getName), clusteringColumns.map(_.getName)))

}
