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
package query.mapper

import java.nio.ByteBuffer

import cats.MonadError
import com.datastax.driver.core.Row
import freestyle.cassandra.codecs.ByteBufferCodec
import freestyle.cassandra.query._
import shapeless._
import shapeless.labelled.{FieldBuilder, FieldType}

trait FromReader[A] {
  def apply[M[_]](reader: ByteBufferReader)(implicit ME: MonadError[M, Throwable]): M[A]
}

case class DatastaxRowReader(row: Row) extends ByteBufferReader {
  override def read[M[_]](name: String)(implicit ME: MonadError[M, Throwable]): M[ByteBuffer] =
    ME.catchNonFatal(row.getBytesUnsafe(name))
}

trait GenericFromReader {

  implicit val hnilFromReader: FromReader[HNil] = new FromReader[HNil] {
    override def apply[M[_]](reader: ByteBufferReader)(
        implicit ME: MonadError[M, Throwable]): M[HNil] = ME.pure(HNil)
  }

  implicit def hconsFromReader[K <: Symbol, V, L <: HList](
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

  implicit def productFromReader[A, L <: HList](
      implicit
      gen: LabelledGeneric.Aux[A, L],
      grL: FromReader[L]): FromReader[A] =
    new FromReader[A] {
      override def apply[M[_]](reader: ByteBufferReader)(
          implicit ME: MonadError[M, Throwable]): M[A] = ME.map(grL(reader))(gen.from)
    }
}

object GenericFromReader extends GenericFromReader