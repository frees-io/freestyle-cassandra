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

import java.nio.ByteBuffer

import cats.MonadError
import freestyle.cassandra.codecs.ByteBufferCodec
import shapeless._
import shapeless.labelled.FieldType

package object mapper {

  abstract class FieldMapper(val name: String) {
    def serialize[M[_]](implicit E: MonadError[M, Throwable]): M[ByteBuffer]
  }

  trait FieldListMapper[A] {
    def map(a: A): List[FieldMapper]
  }

  trait FieldMapperPrimitive {

    def createFieldMapper[A](f: A => List[FieldMapper]): FieldListMapper[A] =
      new FieldListMapper[A] {
        override def map(a: A): List[FieldMapper] = f(a)
      }

    implicit def primitiveFieldMapper[K <: Symbol, H, T <: HList](
        implicit witness: Witness.Aux[K],
        codec: Lazy[ByteBufferCodec[H]],
        tMapper: FieldListMapper[T]): FieldListMapper[FieldType[K, H] :: T] = {
      val fieldName = witness.value.name
      createFieldMapper { hlist =>
        val fieldMapper = new FieldMapper(fieldName) {
          override def serialize[M[_]](implicit E: MonadError[M, Throwable]): M[ByteBuffer] =
            codec.value.serialize(hlist.head)
        }
        fieldMapper :: tMapper.map(hlist.tail)
      }
    }
  }

  trait FieldMapperGeneric extends FieldMapperPrimitive {

    implicit def genericMapper[A, R](
        implicit gen: LabelledGeneric.Aux[A, R],
        mapper: Lazy[FieldListMapper[R]]): FieldListMapper[A] =
      createFieldMapper(value => mapper.value.map(gen.to(value)))

    implicit val hnilMapper: FieldListMapper[HNil] = new FieldListMapper[HNil] {
      override def map(a: HNil): List[FieldMapper] = Nil
    }

  }

  object FieldListMapper extends FieldMapperGeneric

  object FieldMapperExpanded extends FieldMapperGeneric {

    implicit def hconsMapper[K, H, T <: HList](
        implicit hMapper: Lazy[FieldListMapper[H]],
        tMapper: FieldListMapper[T]): FieldListMapper[FieldType[K, H] :: T] =
      createFieldMapper { hlist => hMapper.value.map(hlist.head) ++ tMapper.map(hlist.tail)
      }
  }

}
