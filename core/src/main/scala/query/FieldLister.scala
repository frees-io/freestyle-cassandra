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

import shapeless._
import shapeless.labelled.FieldType

trait FieldLister[A] {
  val list: List[String]
}

trait FieldListerPrimitive {
  implicit def primitiveFieldLister[K <: Symbol, H, T <: HList](
      implicit witness: Witness.Aux[K],
      tLister: FieldLister[T]): FieldLister[FieldType[K, H] :: T] =
    FieldLister[FieldType[K, H] :: T](witness.value.name :: tLister.list)
}

trait FieldListerGeneric extends FieldListerPrimitive {

  implicit def genericLister[A, R](
      implicit gen: LabelledGeneric.Aux[A, R],
      lister: Lazy[FieldLister[R]]): FieldLister[A] = FieldLister[A](lister.value.list)

  implicit val hnilLister: FieldLister[HNil] = FieldLister[HNil](Nil)

}

object FieldLister extends FieldListerGeneric {
  def apply[A](l: List[String]): FieldLister[A] = new FieldLister[A] {
    override val list: List[String] = l
  }
}

object FieldListerExpanded extends FieldListerGeneric {

  implicit def hconsLister[K, H, T <: HList](
      implicit hLister: Lazy[FieldLister[H]],
      tLister: FieldLister[T]): FieldLister[FieldType[K, H] :: T] =
    FieldLister[FieldType[K, H] :: T](hLister.value.list ++ tLister.list)

}
