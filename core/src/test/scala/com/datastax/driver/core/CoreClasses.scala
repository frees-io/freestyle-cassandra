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

package com.datastax.driver.core
import java.nio.ByteBuffer
import java.util.concurrent.{Executor, TimeUnit}

import com.google.common.util.concurrent.ListenableFuture
import freestyle.cassandra.TestUtils._

object CloseFutureTest extends CloseFuture {
  override def force(): CloseFuture = this

  override def cancel(mayInterruptIfRunning: Boolean): Boolean = true

  override def setFuture(future: ListenableFuture[_ <: Void]): Boolean = true

  override def interruptTask(): Unit = {}

  override def get(timeout: Long, unit: TimeUnit): Void = Null[Void]

  override def get(): Void = Null[Void]

  override def setException(throwable: Throwable): Boolean = true

  override def addListener(listener: Runnable, executor: Executor): Unit =
    listener.run()
  override def isCancelled: Boolean = false

  override def set(value: Void): Boolean = true

  override def isDone: Boolean = true
}

case class ResultSetFutureTest(rs: ResultSet) extends ResultSetFuture {
  override def cancel(mayInterruptIfRunning: Boolean): Boolean = true

  override def getUninterruptibly: ResultSet = rs

  override def getUninterruptibly(timeout: Long, unit: TimeUnit): ResultSet = rs

  override def addListener(listener: Runnable, executor: Executor): Unit =
    listener.run()

  override def isCancelled: Boolean = false

  override def isDone: Boolean = true

  override def get(): ResultSet = rs

  override def get(timeout: Long, unit: TimeUnit): ResultSet = rs
}

class StatementTest extends Statement {
  override def getRoutingKey(
      protocolVersion: ProtocolVersion,
      codecRegistry: CodecRegistry): ByteBuffer = Null[ByteBuffer]

  override def getKeyspace: String = Null[String]
}

object MetadataTest extends Metadata(null)

object MetricsTest
    extends Metrics(new Cluster.Builder().addContactPoint("127.0.0.1").build().manager)

case class KeyspaceMetadataTest(name: String, replication: java.util.Map[String, String])
    extends KeyspaceMetadata(name, false, replication)

case class TableMetadataTest(
    ks: KeyspaceMetadata,
    n: String,
    pKey: java.util.List[ColumnMetadata],
    clusteringCols: java.util.List[ColumnMetadata],
    cols: java.util.List[ColumnMetadata])
    extends AbstractTableMetadata(
      Null[KeyspaceMetadata],
      Null[String],
      Null[java.util.UUID],
      Null[java.util.List[ColumnMetadata]],
      Null[java.util.List[ColumnMetadata]],
      Null[java.util.Map[String, ColumnMetadata]],
      Null[TableOptionsMetadata],
      Null[java.util.List[ClusteringOrder]],
      Null[VersionNumber]
    ) {
  override def asCQLQuery(formatted: Boolean): String               = ""
  override def getKeyspace: KeyspaceMetadata                        = ks
  override def getName: String                                      = n
  override def getColumns: java.util.List[ColumnMetadata]           = cols
  override def getClusteringColumns: java.util.List[ColumnMetadata] = clusteringCols
  override def getPartitionKey: java.util.List[ColumnMetadata]      = pKey
}

case class RawTest(n: String, staticColumn: Boolean)
    extends ColumnMetadata.Raw(
      n,
      if (staticColumn) ColumnMetadata.Raw.Kind.STATIC else ColumnMetadata.Raw.Kind.REGULAR,
      0,
      "",
      false)

object ColumnMetadataTest {
  def apply(name: String, dataType: DataType, staticColumn: Boolean): ColumnMetadata =
    ColumnMetadata.fromRaw(Null[TableMetadata], RawTest(name, staticColumn), dataType)
}

object IndexMetadataTest {

  case class RowIndexTest(
      indexName: Option[String],
      target: String,
      indexClassName: Option[String])
      extends AbstractGettableData(ProtocolVersion.V5)
      with Row {

    val optionsMap: java.util.Map[String, String] = new java.util.HashMap[String, String]
    optionsMap.put(IndexMetadata.TARGET_OPTION_NAME, target)
    indexClassName foreach (clazz => optionsMap.put(IndexMetadata.CUSTOM_INDEX_OPTION_NAME, clazz))

    override def getIndexOf(name: String): Int           = -1
    override def getName(i: Int): String                 = Null[String]
    override def getCodecRegistry: CodecRegistry         = Null[CodecRegistry]
    override def getType(i: Int): DataType               = Null[DataType]
    override def getValue(i: Int): ByteBuffer            = Null[ByteBuffer]
    override def getColumnDefinitions: ColumnDefinitions = Null[ColumnDefinitions]
    override def getPartitionKeyToken: Token             = Null[Token]
    override def getToken(i: Int): Token                 = Null[Token]
    override def getToken(name: String): Token           = Null[Token]

    override def getString(name: String): String = name match {
      case IndexMetadata.NAME => indexName.getOrElse(Null[String])
      case IndexMetadata.KIND => IndexMetadata.Kind.KEYS.name()
      case _                  => Null[String]
    }

    override def getMap[K, V](
        name: String,
        keysClass: Class[K],
        valuesClass: Class[V]): java.util.Map[K, V] = name match {
      case IndexMetadata.OPTIONS => optionsMap.asInstanceOf[java.util.Map[K, V]]
      case _                     => Null[java.util.Map[K, V]]
    }

  }

  def apply(
      indexName: Option[String],
      target: String,
      indexClassName: Option[String]): IndexMetadata =
    IndexMetadata.fromRow(Null[TableMetadata], RowIndexTest(indexName, target, indexClassName))
}

class UserTypeTest(
    keyspaceName: String,
    typeName: String,
    fields: java.util.Collection[UserType.Field])
    extends UserType(
      keyspaceName,
      typeName,
      false,
      fields,
      Null[ProtocolVersion],
      Null[CodecRegistry])

object UserTypeTest {
  def apply(
      keyspaceName: String,
      typeName: String,
      fields: java.util.Collection[UserType.Field]): UserTypeTest =
    new UserTypeTest(keyspaceName, typeName, fields)
}

class UserTypeTestDefault extends UserTypeTest("", "", new java.util.ArrayList[UserType.Field](0))

case class UserTypeFieldTest(name: String, dataType: DataType)
    extends UserType.Field(name, dataType)
