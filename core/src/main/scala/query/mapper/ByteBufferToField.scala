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
package query.mapper

import java.nio.ByteBuffer

import cats.MonadError
import freestyle.cassandra.codecs.ByteBufferCodec
import freestyle.cassandra.query.Printer
import shapeless._
import shapeless.labelled.{FieldBuilder, FieldType}
import shapeless.{::, HList, Witness}

trait ByteBufferReader {
  def read[M[_]](name: String)(implicit ME: MonadError[M, Throwable]): M[ByteBuffer]
}

trait FromReader[A] {
  def apply[M[_]](reader: ByteBufferReader)(implicit ME: MonadError[M, Throwable]): M[A]
}

trait GenericFromRow {

  implicit val hnilFromRow: FromReader[HNil] = new FromReader[HNil] {
    override def apply[M[_]](reader: ByteBufferReader)(
        implicit ME: MonadError[M, Throwable]): M[HNil] = ME.pure(HNil)
  }

  implicit def hconsFromRow[K <: Symbol, V, L <: HList](
      implicit
      witness: Witness.Aux[K],
      codec: ByteBufferCodec[V],
      grT: FromReader[L],
      printer: Printer): FromReader[FieldType[K, V] :: L] =
    new FromReader[FieldType[K, V] :: L] {
      override def apply[M[_]](reader: ByteBufferReader)(
          implicit ME: MonadError[M, Throwable]): M[FieldType[K, V] :: L] = {
        val newName = printer.print(witness.value.name)
        ME.flatMap(reader.read(newName)) { byteBuffer =>
          ME.map2(codec.deserialize(byteBuffer), grT(reader)) {
            case (result, l) => new FieldBuilder[K].apply(result) :: l
          }
        }
      }
    }

  implicit def productFromRow[A, L <: HList](
      implicit
      gen: LabelledGeneric.Aux[A, L],
      grL: FromReader[L]): FromReader[A] =
    new FromReader[A] {
      override def apply[M[_]](reader: ByteBufferReader)(
          implicit ME: MonadError[M, Throwable]): M[A] = ME.map(grL(reader))(gen.from)
    }
}

object GenericFromRow extends GenericFromRow

//object Test extends App {
//
//  import com.datastax.driver.core.{ProtocolVersion, TypeCodec}
//  import freestyle.cassandra.query.mapper.ByteBufferToField._
//
//  object GenericFromRow extends GenericFromRow
//  import GenericFromRow._
//
//  case class User(name: String, age: Int)
//
//  implicit val protocolVersion: ProtocolVersion   = ProtocolVersion.V4
//  implicit val stringTypeCodec: TypeCodec[String] = TypeCodec.ascii()
//
//  implicit val printer: Printer = ByteBufferToField.identityPrinter
//
//  val fromReader: FromReader[User] = implicitly[FromReader[User]]
//
//  val nameByteBuffer = stringTypeCodec.serialize("Username", protocolVersion)
//  val ageByteBuffer  = TypeCodec.cint().serialize(34, protocolVersion)
//
//  val reader = new ByteBufferReader() {
//    override def read[M[_]](name: String)(implicit ME: MonadError[M, Throwable]): M[ByteBuffer] =
//      name match {
//        case "name" => ME.pure(nameByteBuffer)
//        case "age"  => ME.pure(ageByteBuffer)
//      }
//  }
//
//  import cats.instances.try_._
//  val result = fromReader[Try](reader)
//
//  println(result)
//}
