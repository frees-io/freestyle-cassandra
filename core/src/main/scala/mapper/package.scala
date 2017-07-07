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

import freestyle.cassandra.codecs.ByteBufferCodec
import shapeless._
import shapeless.labelled.FieldType

package object mapper {

  type MappedField = (String, ByteBuffer)

  trait FieldMapper[A] {
    def map(a: A): List[((MappedField))]
  }

  trait FieldMapperPrimitive {

    def createFieldMapper[A](f: A => List[((MappedField))]): FieldMapper[A] =
      new FieldMapper[A] {
        override def map(a: A): List[((MappedField))] = f(a)
      }

    implicit def primitiveFieldMapper[K <: Symbol, H, T <: HList](
        implicit witness: Witness.Aux[K],
        codec: Lazy[ByteBufferCodec[H]],
        tLister: FieldMapper[T]): FieldMapper[FieldType[K, H] :: T] = {
      val fieldName = witness.value.name
      createFieldMapper { hlist =>
        (fieldName -> codec.value.serialize(hlist.head)) :: tLister.map(hlist.tail)
      }
    }
  }

  trait FieldMapperGeneric extends FieldMapperPrimitive {

    implicit def genericMapper[A, R](
        implicit gen: LabelledGeneric.Aux[A, R],
        mapper: Lazy[FieldMapper[R]]): FieldMapper[A] =
      createFieldMapper(value => mapper.value.map(gen.to(value)))

    implicit val hnilLister: FieldMapper[HNil] = new FieldMapper[HNil] {
      override def map(a: HNil): List[((MappedField))] = Nil
    }

  }

  object FieldMapper extends FieldMapperGeneric

  object FieldMapperExpanded extends FieldMapperGeneric {

    implicit def hconsLister[K, H, T <: HList](
        implicit hLister: Lazy[FieldMapper[H]],
        tLister: FieldMapper[T]): FieldMapper[FieldType[K, H] :: T] =
      createFieldMapper { hlist =>
        hLister.value.map(hlist.head) ++ tLister.map(hlist.tail)
      }
  }

}
