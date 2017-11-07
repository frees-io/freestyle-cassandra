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
package query

import java.nio.ByteBuffer

import cats.MonadError
import com.datastax.driver.core._
import freestyle.cassandra.query.mapper.FieldListMapper
import freestyle.cassandra.query.model.{SerializableValue, SerializableValueBy}
import org.scalacheck.{Arbitrary, Gen}
import freestyle.cassandra.schema.MetadataArbitraries._
import troy.cql.ast.ddl.Table
import troy.cql.ast.{DataType => TroyDataType}

import scala.util.Try

trait QueryArbitraries {

  implicit val arbitraryPrinter: Arbitrary[Printer] = Arbitrary {
    Gen.oneOf(List(identityPrinter, lowerCasePrinter, upperCasePrinter))
  }

  val byteBufferIntGen: Gen[ByteBuffer] =
    Gen.posNum[Int].map(n => TypeCodec.cint().serialize(n, ProtocolVersion.V3))

  val byteBufferBooleanGen: Gen[ByteBuffer] =
    implicitly[Arbitrary[Boolean]].arbitrary.map { n =>
      TypeCodec.cboolean().serialize(n, ProtocolVersion.V3)
    }

  val byteBufferFloatGen: Gen[ByteBuffer] =
    Gen.posNum[Float].map(n => TypeCodec.cfloat().serialize(n, ProtocolVersion.V3))

  val byteBufferUUIDGen: Gen[ByteBuffer] =
    Gen.uuid.map(n => TypeCodec.uuid().serialize(n, ProtocolVersion.V3))

  val byteBufferStringGen: Gen[ByteBuffer] =
    Gen.alphaStr.map(n => TypeCodec.varchar().serialize(n, ProtocolVersion.V3))

  val byteBufferGen: Gen[ByteBuffer] =
    Gen.oneOf(
      byteBufferIntGen,
      byteBufferBooleanGen,
      byteBufferFloatGen,
      byteBufferUUIDGen,
      byteBufferStringGen)

  val selectQueryGen: Gen[String] = {

    val createSelect: (List[Table.Column]) => String = {
      case Nil => "*"
      case l   => l.map(_.name).mkString(",")
    }

    val generateType: (TroyDataType) => Gen[String] = {
      case TroyDataType.Ascii    => Gen.alphaStr.map(s => s"'$s'")
      case TroyDataType.BigInt   => Gen.posNum[Long].map(_.toString)
      case TroyDataType.Boolean  => Gen.oneOf(true, false).map(_.toString)
      case TroyDataType.Decimal  => Gen.posNum[Float].map(_.toString)
      case TroyDataType.Double   => Gen.posNum[Float].map(_.toString)
      case TroyDataType.Float    => Gen.posNum[Float].map(_.toString)
      case TroyDataType.Int      => Gen.posNum[Int].map(_.toString)
      case TroyDataType.Smallint => Gen.posNum[Short].map(_.toString)
      case TroyDataType.Text     => Gen.alphaStr.map(s => s"'$s'")
      case TroyDataType.Tinyint  => Gen.posNum[Byte].map(_.toString)
      case TroyDataType.Uuid     => Gen.uuid.map(_.toString)
      case TroyDataType.Varchar  => Gen.alphaStr.map(s => s"'$s'")
      case _                     => Gen.const("null")
    }

    val createFilter: (List[Table.Column]) => Gen[String] = {
      case Nil => Gen.const("")
      case l =>
        l.foldLeft(Gen.const(List.empty[String])) {
            case (genS, c) =>
              genS.flatMap { strings =>
                generateType(c.dataType).map(data => strings :+ s"${c.name} = $data")
              }
          }
          .map(_.mkString("WHERE ", " AND ", ""))
    }

    for {
      generatedTable <- generatedTableArb(None).arbitrary
      selectColumns <- Gen
        .someOf(generatedTable.createTable.columns)
        .map(seq => createSelect(seq.toList))
      filteredColumns <- Gen
        .someOf(generatedTable.createTable.columns)
        .flatMap(seq => createFilter(seq.toList))
    } yield {
      s"""
         | SELECT $selectColumns
         | FROM ${generatedTable.createTable.tableName}
         | $filteredColumns
     """.stripMargin
    }
  }

  val dataGen: Gen[Map[String, String]] =
    Gen.mapOf {
      for {
        key   <- Gen.alphaStr
        value <- Gen.alphaStr
      } yield (key, value)
    }

  implicit val serializableValueByIntListArb: Arbitrary[
    List[(ByteBuffer, SerializableValueBy[Int])]] =
    Arbitrary {
      Gen.listOf(byteBufferGen).map { list =>
        list.zipWithIndex.map {
          case (byteBuffer, index) =>
            val value = new SerializableValue {
              override def serialize[M[_]](implicit E: MonadError[M, Throwable]): M[ByteBuffer] =
                E.pure(byteBuffer)
            }
            (byteBuffer, SerializableValueBy(index, value))
        }
      }
    }

  implicit def rowAndDataGen[A](
      implicit arb: Arbitrary[A],
      fieldListMapper: FieldListMapper[A],
      fieldLister: FieldLister[A],
      printer: Printer): Gen[(ResultSet, List[A])] =
    Gen.listOf(arb.arbitrary).map { list =>
      import cats.instances.try_._
      import cats.instances.list._
      import cats.syntax.traverse._
      val rows = list.map { element =>
        val byteBufferList = fieldListMapper.map(element).traverse(_.serialize[Try]).get
        ListBackedRow(fieldLister.list, byteBufferList)
      }
      (ResultSetBuilder(fieldLister.list, rows), list)
    }
}

object QueryArbitraries extends QueryArbitraries
