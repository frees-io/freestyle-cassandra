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
package mapper

import java.nio.ByteBuffer

import cats.MonadError
import com.datastax.driver.core.{ProtocolVersion, TypeCodec}
import org.scalacheck.Prop._
import org.scalatest.{Matchers, WordSpec}
import org.scalacheck.Shapeless._
import org.scalatest.prop.Checkers

import scala.util.matching.Regex

class ByteBufferMapperSpec extends WordSpec with Matchers with Checkers {

  case class A(a1: Int, a2: String, a3: Boolean)

  case class B(b1: Long, b2: String)

  case class C(c1: String, c2: B)

  implicit val stringTypeCodec: TypeCodec[String] = TypeCodec.varchar()
  implicit val protocol: ProtocolVersion          = ProtocolVersion.V3
  import codecs._

  val stringCodec: ByteBufferCodec[String] = implicitly[ByteBufferCodec[String]]

  "ByteBufferMapper.map" should {

    "map the fields for a regular case class" in {

      import FieldListMapper._
      import cats.instances.try_._

      check {
        forAll { a: A =>
          val mapper: ByteBufferMapper[A]   = ByteBufferMapper[A]
          val mapperList: List[FieldMapper] = mapper.map(a)
          mapperList.size == 3 &&
          mapperList.head.serialize == intCodec.serialize(a.a1) &&
          mapperList(1).serialize == stringCodec.serialize(a.a2) &&
          mapperList(2).serialize == booleanCodec.serialize(a.a3)
        }
      }

    }

    "map the fields for a case class with another embedded case class and his decoder" in {

      import FieldListMapper._

      implicit val decoder: ByteBufferCodec[B] = new ByteBufferCodec[B] {

        val Regex: Regex = "(\\d+);(.+)".r

        override def deserialize[M[_]](bytes: ByteBuffer)(
            implicit M: MonadError[M, Throwable]): M[B] =
          M.flatMap(stringCodec.deserialize(bytes)) {
            case Regex(v1, v2) => M.pure(B(v1.toLong, v2))
            case _             => M.raiseError[B](new RuntimeException("Bad serialized value"))
          }

        override def serialize[M[_]](value: B)(
            implicit M: MonadError[M, Throwable]): M[ByteBuffer] =
          stringCodec.serialize(value.b1 + ";" + value.b2)
      }

      import cats.instances.try_._

      check {
        forAll { c: C =>
          val mapper: ByteBufferMapper[C]   = ByteBufferMapper[C]
          val mapperList: List[FieldMapper] = mapper.map(c)
          mapperList.size == 2 &&
          mapperList.head.serialize == stringCodec.serialize(c.c1) &&
          mapperList(1).serialize == decoder.serialize(c.c2)
        }
      }

    }

    "map the fields for a case class with another embedded case class" in {

      import FieldMapperExpanded._
      import cats.instances.try_._

      check {
        forAll { c: C =>
          val mapper                        = ByteBufferMapper[C]
          val mapperList: List[FieldMapper] = mapper.map(c)
          mapperList.size == 3 &&
          mapperList.head.serialize == stringCodec.serialize(c.c1) &&
          mapperList(1).serialize == longCodec.serialize(c.c2.b1) &&
          mapperList(2).serialize == stringCodec.serialize(c.c2.b2)
        }
      }

    }

  }

}
