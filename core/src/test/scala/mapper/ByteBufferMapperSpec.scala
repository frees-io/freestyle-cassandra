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

      check {
        forAll { a: A =>
          val mapper = ByteBufferMapper[A]
          mapper.map(a).toMap == Map(
            "a1" -> intCodec.serialize(a.a1),
            "a2" -> stringCodec.serialize(a.a2),
            "a3" -> booleanCodec.serialize(a.a3)
          )
        }
      }

    }

    "map the fields for a case class with another embedded case class and his decoder" in {

      import FieldListMapper._

      implicit val decoder: ByteBufferCodec[B] = new ByteBufferCodec[B] {

        val Regex: Regex = "(\\d+);(.+)".r

        override def deserialize(bytes: ByteBuffer): B =
          stringCodec.deserialize(bytes) match {
            case Regex(v1, v2) => B(v1.toLong, v2)
            case _             => throw new RuntimeException("Bad serialized value")
          }

        override def serialize(value: B): ByteBuffer =
          stringCodec.serialize(value.b1 + ";" + value.b2)
      }

      check {
        forAll { c: C =>
          val mapper = ByteBufferMapper[C]
          mapper.map(c).toMap == Map(
            "c1" -> stringCodec.serialize(c.c1),
            "c2" -> decoder.serialize(c.c2)
          )
        }
      }

    }

    "map the fields for a case class with another embedded case class" in {

      import FieldMapperExpanded._

      check {
        forAll { c: C =>
          val mapper = ByteBufferMapper[C]
          mapper.map(c).toMap == Map(
            "c1" -> stringCodec.serialize(c.c1),
            "b1" -> longCodec.serialize(c.c2.b1),
            "b2" -> stringCodec.serialize(c.c2.b2)
          )
        }
      }

    }

  }

}
