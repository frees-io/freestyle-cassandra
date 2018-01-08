/*
 * Copyright 2017-2018 47 Degrees, LLC. <http://www.47deg.com>
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

import cats.data.NonEmptyList
import com.datastax.driver.core.{
  AbstractTableMetadata,
  CodecRegistry,
  ColumnMetadata,
  ColumnMetadataTest,
  IndexMetadata,
  IndexMetadataTest,
  KeyspaceMetadata,
  KeyspaceMetadataTest,
  ProtocolVersion,
  TableMetadataTest,
  TupleType,
  UserType,
  UserTypeFieldTest,
  UserTypeTest,
  DataType => DatastaxDataType
}
import freestyle.cassandra.TestUtils.Null
import org.scalacheck.{Arbitrary, Gen}
import troy.cql.ast.ddl.{Field, Index, Keyspace, Table}
import troy.cql.ast.dml.{Operator, Select, WhereClause}
import troy.cql.ast.{
  CreateIndex,
  CreateKeyspace,
  CreateTable,
  CreateType,
  KeyspaceName,
  SelectStatement,
  StringConstant,
  TableName,
  TypeName,
  DataType => TroyDataType
}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.Random

trait MetadataArbitraries {

  case class GeneratedKeyspace(
      cql: String,
      createKeyspace: CreateKeyspace,
      keyspaceMetadata: KeyspaceMetadata)

  case class GeneratedTable(
      cql: String,
      createTable: CreateTable,
      tableMetadata: AbstractTableMetadata)

  case class GeneratedKeyspaceAndTable(
      cql: String,
      generatedKeyspace: GeneratedKeyspace,
      generatedTable: GeneratedTable)

  case class GeneratedIndex(cql: String, createIndex: CreateIndex, indexMetadata: IndexMetadata)

  case class GeneratedUserType(cql: String, createType: CreateType, userType: UserType)

  trait ArbDataType {
    val name: String
    val troyType: TroyDataType
    val datastaxType: DatastaxDataType
  }

  object ArbDataType {
    def apply(n: String, t: TroyDataType, d: DatastaxDataType): ArbDataType =
      new ArbDataType {
        override val name: String                   = n
        override val troyType: TroyDataType         = t
        override val datastaxType: DatastaxDataType = d
      }
  }

  case class ArbNativeDataType(
      name: String,
      troyType: TroyDataType.Native,
      datastaxType: DatastaxDataType)
      extends ArbDataType

  case class GeneratedStatement(
      keyspace: CreateKeyspace,
      table: CreateTable,
      validStatement: (String, SelectStatement),
      invalidStatement: (String, SelectStatement))

  case class GeneratedKeyspaceWithTables(
      keyspace: CreateKeyspace,
      validTables: List[CreateTable],
      invalidTables: List[CreateTable])

  val identifierGen: Gen[String] =
    (for {
      size <- Gen.chooseNum[Int](2, 15)
      c    <- Gen.alphaLowerChar
      cs   <- Gen.listOfN(size, Gen.alphaNumChar)
    } yield (c :: cs).mkString)
      .map(_.toLowerCase)
      .filter(name => !TestUtils.reservedKeywords.contains(name.toUpperCase))

  def namedGen[T](implicit gen: Gen[T]): Gen[(String, T)] =
    for {
      name  <- identifierGen
      value <- gen
    } yield (name, value)

  def distinctListOfGen[T](gen: Gen[T], genSize: Gen[Int], maxDiscarded: Int = 1000)(
      comp: (T) => Any): Gen[List[T]] = genSize.map { size =>
    val seen: ListBuffer[T] = new ListBuffer[T]
    var discarded           = 0
    if (size == seen.size) seen.toList
    else {
      while (seen.size <= size && discarded < maxDiscarded) gen.sample match {
        case Some(t) if !seen.exists(comp(_) == comp(t)) =>
          seen.+=:(t)
        case _ => discarded += 1
      }
      seen.toList
    }
  }

  implicit val generatedKeyspaceArb: Arbitrary[GeneratedKeyspace] = {

    def cql(name: String, props: Seq[(String, String)]): String =
      s"""
         |CREATE KEYSPACE $name WITH replication =
         |  { ${props.sortBy(_._1).map(t => s"'${t._1}': '${t._2}'").mkString(",")}};
    """.stripMargin

    val simpleStrategyGen: Gen[Seq[(String, String)]] =
      Gen.posNum[Int].map { factor =>
        Seq(("class", "SimpleStrategy"), ("replication_factor", factor.toString))
      }

    val networkTopologyGen: Gen[Seq[(String, String)]] =
      Gen.listOf(Gen.posNum[Int]).map { factorList =>
        ("class", "NetworkTopologyStrategy") +: factorList.zipWithIndex.map {
          case (f, i) => (s"DC$i", f.toString)
        }
      }

    Arbitrary {
      for {
        name  <- identifierGen
        props <- Gen.oneOf(simpleStrategyGen, networkTopologyGen)
      } yield
        GeneratedKeyspace(
          cql(name, props),
          CreateKeyspace(
            ifNotExists = false,
            keyspaceName = KeyspaceName(name),
            properties = Seq(Keyspace.Replication(props.sortBy(_._1)))
          ),
          KeyspaceMetadataTest(name, props.toMap.asJava)
        )
    }
  }

  val nativeDataTypeGen: Gen[ArbNativeDataType] =
    Gen
      .oneOf(
        ("ASCII", TroyDataType.Ascii, DatastaxDataType.ascii()),
        ("BIGINT", TroyDataType.BigInt, DatastaxDataType.bigint()),
        ("BLOB", TroyDataType.Blob, DatastaxDataType.blob()),
        ("BOOLEAN", TroyDataType.Boolean, DatastaxDataType.cboolean()),
        ("COUNTER", TroyDataType.Counter, DatastaxDataType.counter()),
        ("DATE", TroyDataType.Date, DatastaxDataType.date()),
        ("DECIMAL", TroyDataType.Decimal, DatastaxDataType.decimal()),
        ("DOUBLE", TroyDataType.Double, DatastaxDataType.cdouble()),
        ("FLOAT", TroyDataType.Float, DatastaxDataType.cfloat()),
        ("INET", TroyDataType.Inet, DatastaxDataType.inet()),
        ("INT", TroyDataType.Int, DatastaxDataType.cint()),
        ("SMALLINT", TroyDataType.Smallint, DatastaxDataType.smallint()),
        ("TEXT", TroyDataType.Text, DatastaxDataType.text()),
        ("TIME", TroyDataType.Time, DatastaxDataType.time()),
        ("TIMESTAMP", TroyDataType.Timestamp, DatastaxDataType.timestamp()),
        ("TIMEUUID", TroyDataType.Timeuuid, DatastaxDataType.timeuuid()),
        ("TINYINT", TroyDataType.Tinyint, DatastaxDataType.tinyint()),
        ("UUID", TroyDataType.Uuid, DatastaxDataType.uuid()),
        ("VARCHAR", TroyDataType.Varchar, DatastaxDataType.varchar()),
        ("VARINT", TroyDataType.Varint, DatastaxDataType.varint())
      )
      .map(t => ArbNativeDataType.apply(t._1, t._2, t._3))

  val mapDataTypeGen: Gen[ArbDataType] =
    for {
      keyType   <- nativeDataTypeGen
      valueType <- nativeDataTypeGen
    } yield
      ArbDataType(
        s"MAP<${keyType.name}, ${valueType.name}>",
        TroyDataType.Map(keyType.troyType, valueType.troyType),
        DatastaxDataType.map(keyType.datastaxType, valueType.datastaxType)
      )

  val setDataType: Gen[ArbDataType] =
    nativeDataTypeGen.map {
      case ArbNativeDataType(s, t, d) =>
        ArbDataType(s"SET<$s>", TroyDataType.Set(t), DatastaxDataType.set(d))
    }

  val listDataType: Gen[ArbDataType] =
    nativeDataTypeGen.map {
      case ArbNativeDataType(s, t, d) =>
        ArbDataType(s"LIST<$s>", TroyDataType.List(t), DatastaxDataType.list(d))
    }

  val tupleDataType: Gen[ArbDataType] =
    Gen.nonEmptyListOf(nativeDataTypeGen).map { typeList =>
      ArbDataType(
        s"TUPLE<${typeList.map(_.name).mkString(",")}>",
        TroyDataType.Tuple(typeList.map(_.troyType)),
        TupleType.of(ProtocolVersion.V5, Null[CodecRegistry], typeList.map(_.datastaxType): _*)
      )
    }

  implicit val dataTypeGen: Gen[ArbDataType] =
    Gen.oneOf(nativeDataTypeGen, mapDataTypeGen, setDataType, listDataType, tupleDataType)

  implicit val generatedTableArbitrary: Arbitrary[GeneratedTable] = generatedTableArb()

  def generatedTableArb(
      maybeKeyspace: Option[GeneratedKeyspace] = None): Arbitrary[GeneratedTable] = {

    def tableCQL(
        keyspace: String,
        table: String,
        partitions: Seq[(String, Boolean, ArbDataType)],
        clustering: Seq[(String, Boolean, ArbDataType)],
        columns: Seq[(String, Boolean, ArbDataType)]): String = {

      def colDef(col: (String, Boolean, ArbDataType)): String =
        s"${col._1} ${col._3.name} ${if (col._2) "STATIC" else ""}"

      val clusteringString =
        if (clustering.isEmpty) ""
        else {
          clustering.map(_._1).mkString(", ", ", ", "")
        }

      s"""
         |CREATE TABLE $keyspace.$table (
         |  ${(partitions ++ clustering ++ columns).map(colDef).mkString(",")},
         |  PRIMARY KEY (${partitions.map(_._1).mkString("(", ",", ")")}$clusteringString)
         |);
       """.stripMargin
    }

    def sampleStatic(
        columnDef: (String, ArbDataType),
        staticProb: Double): (String, Boolean, ArbDataType) =
      (columnDef._1, Random.nextDouble() < staticProb, columnDef._2)

    def toTroyColumns(columnDef: (String, Boolean, ArbDataType)): Table.Column =
      Table.Column(
        name = columnDef._1,
        dataType = columnDef._3.troyType,
        isStatic = columnDef._2,
        isPrimaryKey = false)

    def toDatastaxColumns(columnDef: (String, Boolean, ArbDataType)): ColumnMetadata =
      ColumnMetadataTest(columnDef._1, columnDef._3.datastaxType, columnDef._2)

    Arbitrary {

      for {
        keyspace <- maybeKeyspace map Gen.const getOrElse generatedKeyspaceArb.arbitrary
        keyspaceName = keyspace.createKeyspace.keyspaceName.name
        name         <- identifierGen
        numPartKeys  <- Gen.chooseNum(1, 5)
        numClustKeys <- Gen.chooseNum(0, 5)
        numColumns   <- Gen.chooseNum(0, 10)
        columnNames <- distinctListOfGen(
          identifierGen,
          Gen.const(numPartKeys + numClustKeys + numColumns))(v => v)
        part    <- Gen.listOfN(numPartKeys, nativeDataTypeGen)
        clust   <- Gen.listOfN(numClustKeys, nativeDataTypeGen)
        columns <- Gen.listOfN(numColumns, dataTypeGen)
        namedPart  = columnNames.take(numPartKeys).zip(part)
        namedClust = columnNames.slice(numPartKeys, numPartKeys + numClustKeys).zip(clust)
        namedCol   = columnNames.takeRight(numColumns).zip(columns)
      } yield {

        val partitionsWithStatic = namedPart.map(sampleStatic(_, 0))
        val clusteringWithStatic = namedClust.map(sampleStatic(_, 0))
        val columnsWithStatic    = namedCol.map(sampleStatic(_, 0.2))

        val troyColumns = (partitionsWithStatic ++ clusteringWithStatic).map(toTroyColumns) ++ columnsWithStatic
          .map(toTroyColumns)
        val troyPrimaryKey = Table.PrimaryKey(namedPart.map(_._1), namedClust.map(_._1))

        val datastaxPartitions = partitionsWithStatic.map(toDatastaxColumns)
        val datastaxClustering = clusteringWithStatic.map(toDatastaxColumns)
        val datastaxColumns = datastaxPartitions ++ datastaxClustering ++ columnsWithStatic.map(
          toDatastaxColumns)

        GeneratedTable(
          tableCQL(
            keyspaceName,
            name,
            partitionsWithStatic,
            clusteringWithStatic,
            columnsWithStatic),
          CreateTable(
            ifNotExists = false,
            tableName = TableName(Some(keyspace.createKeyspace.keyspaceName), name),
            columns = troyColumns,
            primaryKey = Some(troyPrimaryKey),
            options = Seq.empty
          ),
          TableMetadataTest(
            ks = keyspace.keyspaceMetadata,
            n = name,
            pKey = datastaxPartitions.asJava,
            clusteringCols = datastaxClustering.asJava,
            cols = datastaxColumns.asJava
          )
        )
      }

    }
  }

  implicit val generatedTableList: Arbitrary[GeneratedKeyspaceAndTable] = Arbitrary {
    for {
      keyspace <- generatedKeyspaceArb.arbitrary
      table    <- generatedTableArb(Some(keyspace)).arbitrary
    } yield
      GeneratedKeyspaceAndTable(
        s"""${keyspace.cql}
           |${table.cql}""".stripMargin,
        keyspace,
        table
      )
  }

  implicit val generatedIndexArb: Arbitrary[GeneratedIndex] = {

    def indexCQL(
        keyspaceName: Option[String],
        tableName: String,
        target: String,
        indexName: Option[String],
        className: Option[String]): String =
      s"""
         |CREATE ${if (className.isDefined) "CUSTOM " else ""} INDEX ${indexName.getOrElse("")}
         |ON ${keyspaceName.map(s => s"$s.").getOrElse("")}$tableName
         |${className.map(s => s"USING '$s'").getOrElse("")};
       """.stripMargin

    Arbitrary {
      for {
        keyspaceName <- Gen.option(identifierGen)
        tableName    <- identifierGen
        target       <- identifierGen
        name         <- Gen.option(identifierGen)
        custom       <- Arbitrary.arbitrary[Boolean]
        usingClass   <- if (custom) Gen.identifier.map(Option(_)) else Gen.const(None)
      } yield
        GeneratedIndex(
          indexCQL(keyspaceName, tableName, target, name, usingClass),
          CreateIndex(
            isCustom = custom,
            ifNotExists = false,
            indexName = name,
            tableName = TableName(keyspaceName.map(KeyspaceName), tableName),
            identifier = Index.Identifier(target),
            using = usingClass map (Index.Using(_, None))
          ),
          IndexMetadataTest(name, target, usingClass)
        )
    }
  }

  implicit val generatedUserTypeArb: Arbitrary[GeneratedUserType] = {

    def userTypeCQL(
        keyspace: String,
        typeName: String,
        types: Seq[(String, ArbDataType)]): String = {

      def typeDef(col: (String, ArbDataType)): String =
        s"${col._1} ${col._2.name}"

      s"""
         |CREATE TYPE $keyspace.$typeName (
         |  ${types.map(typeDef).mkString(",")}
         |);
       """.stripMargin
    }

    Arbitrary {
      for {
        keyspaceName   <- identifierGen
        typeName       <- identifierGen
        innerTypeNames <- distinctListOfGen(identifierGen, Gen.chooseNum(1, 10))(name => name)
        innerTypes     <- Gen.listOfN(innerTypeNames.size, dataTypeGen)
      } yield {

        val types = innerTypeNames.zip(innerTypes)

        val (troyFields, datastaxFields): (List[Field], List[UserType.Field]) = types.map {
          case (name, arbDataType) =>
            (Field(name, arbDataType.troyType), UserTypeFieldTest(name, arbDataType.datastaxType))
        }.unzip

        GeneratedUserType(
          userTypeCQL(keyspaceName, typeName, types),
          CreateType(
            ifNotExists = false,
            typeName = TypeName(Some(KeyspaceName(keyspaceName)), typeName),
            fields = troyFields),
          UserTypeTest(
            keyspaceName = keyspaceName,
            typeName = typeName,
            fields = datastaxFields.asJavaCollection)
        )
      }
    }

  }

  implicit val generatedStatementArb: Arbitrary[GeneratedStatement] = {

    def statementGen(
        keyspaceName: Option[KeyspaceName],
        tableName: TableName,
        columnNames: Seq[String],
        valid: Boolean): Gen[(String, SelectStatement)] = {

      val cqlKeyspace = keyspaceName.map(n => s"${n.name}.").getOrElse("")

      for {
        columnName <- Gen.oneOf(columnNames)
        selectColumn <- if (valid) Gen.oneOf(columnNames)
        else identifierGen.filter(!columnNames.contains(_))
        columnValue <- Gen.alphaStr
      } yield
        (
          s"""
           |SELECT $selectColumn FROM $cqlKeyspace${tableName.table}
           |WHERE $columnName = '$columnValue'
       """.stripMargin,
          SelectStatement(
            mod = None,
            selection = Select.SelectClause(
              Seq(
                Select.SelectionClauseItem(selector = Select.ColumnName(selectColumn), as = None))),
            from = tableName,
            where = Some(
              WhereClause(
                Seq(
                  WhereClause.Relation
                    .Simple(
                      columnName = columnName,
                      operator = Operator.Equals,
                      term = StringConstant(columnValue))))),
            orderBy = None,
            perPartitionLimit = None,
            limit = None,
            allowFiltering = false
          )
        )

    }

    Arbitrary {
      for {
        keyspace <- generatedKeyspaceArb.arbitrary
        table    <- generatedTableArb(Some(keyspace)).arbitrary
        tableName = table.createTable.tableName
        columns   = table.createTable.columns.map(_.name)
        validSt   <- statementGen(tableName.keyspace, tableName, columns, valid = true)
        invalidSt <- statementGen(tableName.keyspace, tableName, columns, valid = false)
      } yield GeneratedStatement(keyspace.createKeyspace, table.createTable, validSt, invalidSt)
    }

  }

  implicit val generatedKeyspaceWithTablesArb: Arbitrary[GeneratedKeyspaceWithTables] = Arbitrary {
    for {
      keyspaces <- distinctListOfGen[GeneratedKeyspace](
        generatedKeyspaceArb.arbitrary,
        Gen.const(2))(_.createKeyspace.keyspaceName)
      validTables <- distinctListOfGen[GeneratedTable](
        generatedTableArb(Some(keyspaces.head)).arbitrary,
        Gen.chooseNum[Int](1, 3))(_.createTable.tableName)
      invalidTables <- distinctListOfGen[GeneratedTable](
        generatedTableArb(Some(keyspaces.last)).arbitrary,
        Gen.chooseNum[Int](1, 3))(_.createTable.tableName)
    } yield
      GeneratedKeyspaceWithTables(
        keyspaces.head.createKeyspace,
        validTables.map(_.createTable),
        invalidTables.map(_.createTable))
  }

  val schemaGen: Gen[(
      GeneratedKeyspace,
      NonEmptyList[GeneratedTable],
      List[GeneratedIndex],
      List[GeneratedUserType])] =
    for {
      keyspace <- generatedKeyspaceArb.arbitrary
      tables <- distinctListOfGen[GeneratedTable](
        generatedTableArb(Some(keyspace)).arbitrary,
        Gen.chooseNum[Int](1, 3))(_.createTable.tableName)
      indexes <- distinctListOfGen[GeneratedIndex](
        generatedIndexArb.arbitrary,
        Gen.chooseNum[Int](0, 2))(_.createIndex.indexName)
      userTypes <- distinctListOfGen[GeneratedUserType](
        generatedUserTypeArb.arbitrary,
        Gen.chooseNum[Int](0, 3))(_.createType.typeName)
    } yield (keyspace, NonEmptyList(tables.head, tables.tail), indexes, userTypes)

}

object MetadataArbitraries extends MetadataArbitraries
