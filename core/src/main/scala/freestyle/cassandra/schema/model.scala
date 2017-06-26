package freestyle.cassandra.schema

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

}
