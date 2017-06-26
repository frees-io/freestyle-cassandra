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

object model {

  case class TableDefinition(
      keyspace: Option[String],
      name: String,
      columns: List[ColumnDefinition],
      options: List[TableOptions])

  case class ColumnDefinition(name: String, cqlType: CQLType, columnType: ColumnType)

  sealed trait CQLType
  case class CQLList(cqlType: CQLType)                    extends CQLType
  case class CQLMap(cqlTypeK: CQLType, cqlTypeV: CQLType) extends CQLType
  case class CQLSet(cqlType: CQLType)                     extends CQLType
  case class CQLTuple(cqlType: CQLType, other: CQLType*)  extends CQLType
  case object Ascii                                       extends CQLType
  case object BigInt                                      extends CQLType
  case object Blob                                        extends CQLType
  case object Boolean                                     extends CQLType
  case object Counter                                     extends CQLType
  case object Date                                        extends CQLType
  case object Decimal                                     extends CQLType
  case object Double                                      extends CQLType
  case object Duration                                    extends CQLType
  case object Float                                       extends CQLType
  case object Inet                                        extends CQLType
  case object Int                                         extends CQLType
  case object SmallInt                                    extends CQLType
  case object Text                                        extends CQLType
  case object Time                                        extends CQLType
  case object Timestamp                                   extends CQLType
  case object TimeUUID                                    extends CQLType
  case object TinyInt                                     extends CQLType
  case object UDT                                         extends CQLType
  case object UUID                                        extends CQLType
  case object Varchar                                     extends CQLType
  case object VarInt                                      extends CQLType

  sealed trait ColumnType
  case object RegularColumn    extends ColumnType
  case object StaticColumn     extends ColumnType
  case object PrimaryKeyColumn extends ColumnType
  case object ClusteringColumn extends ColumnType

  sealed trait TableOptions
  case class ClusteringOrder(order: List[ColumnOrder])
  case object CompactStorage extends TableOptions

  case class ColumnOrder(columnName: String, asc: Boolean)

  val simpleStrategyClass: String          = "SimpleStrategy"
  val networkTopologyStrategyClass: String = "NetworkTopologyStrategy"

  abstract class KeyspaceReplication(cls: String)

  case class SimpleStrategyReplication(replicationFactor: Int)
      extends KeyspaceReplication(simpleStrategyClass)

  case class NetworkTopologyStrategyReplication(dcs: Map[String, Int])
      extends KeyspaceReplication(networkTopologyStrategyClass)

  case class Keyspace(
      name: String,
      replication: KeyspaceReplication,
      durableWrites: Option[Boolean])

}
