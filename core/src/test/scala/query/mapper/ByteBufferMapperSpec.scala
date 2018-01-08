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
import com.datastax.driver.core.{ProtocolVersion, TypeCodec}
import freestyle.cassandra.query.{Printer, QueryArbitraries}
import org.scalacheck.Prop._
import org.scalatest.{Matchers, WordSpec}
import org.scalacheck.ScalacheckShapeless._
import org.scalatest.prop.Checkers

import scala.util.matching.Regex

class ByteBufferMapperSpec extends WordSpec with Matchers with Checkers with QueryArbitraries {

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
        forAll { (a: A, printer: Printer) =>
          implicit val _                    = printer
          val mapper: ByteBufferMapper[A]   = ByteBufferMapper[A]
          val mapperList: List[FieldMapper] = mapper.map(a)
          mapperList.size == 3 &&
          mapperList.head.name == printer.print("a1") &&
          mapperList.head.serialize == intCodec.serialize(a.a1) &&
          mapperList(1).name == printer.print("a2") &&
          mapperList(1).serialize == stringCodec.serialize(a.a2) &&
          mapperList(2).name == printer.print("a3") &&
          mapperList(2).serialize == booleanCodec.serialize(a.a3)
        }
      }

    }

    "map the fields for a case class with another embedded case class and his decoder" in {

      import FieldListMapper._

      implicit val decoder: ByteBufferCodec[B] = new ByteBufferCodec[B] {

        val Regex: Regex = "(\\d+);(.+)".r

        override def deserialize[M[_]](bytes: ByteBuffer)(
            implicit E: MonadError[M, Throwable]): M[B] =
          E.flatMap(stringCodec.deserialize(bytes)) {
            case Regex(v1, v2) => E.pure(B(v1.toLong, v2))
            case _             => E.raiseError[B](new RuntimeException("Bad serialized value"))
          }

        override def serialize[M[_]](value: B)(
            implicit E: MonadError[M, Throwable]): M[ByteBuffer] =
          stringCodec.serialize(value.b1 + ";" + value.b2)
      }

      import cats.instances.try_._

      check {
        forAll { (c: C, printer: Printer) =>
          implicit val _                    = printer
          val mapper: ByteBufferMapper[C]   = ByteBufferMapper[C]
          val mapperList: List[FieldMapper] = mapper.map(c)
          mapperList.size == 2 &&
          mapperList.head.name == printer.print("c1") &&
          mapperList.head.serialize == stringCodec.serialize(c.c1) &&
          mapperList(1).name == printer.print("c2") &&
          mapperList(1).serialize == decoder.serialize(c.c2)
        }
      }

    }

    "map the fields for a case class with another embedded case class" in {

      import FieldMapperExpanded._
      import cats.instances.try_._

      check {
        forAll { (c: C, printer: Printer) =>
          implicit val _                    = printer
          val mapper                        = ByteBufferMapper[C]
          val mapperList: List[FieldMapper] = mapper.map(c)
          mapperList.size == 3 &&
          mapperList.head.name == printer.print("c1") &&
          mapperList.head.serialize == stringCodec.serialize(c.c1) &&
          mapperList(1).name == printer.print("b1") &&
          mapperList(1).serialize == longCodec.serialize(c.c2.b1) &&
          mapperList(2).name == printer.print("b2") &&
          mapperList(2).serialize == stringCodec.serialize(c.c2.b2)
        }
      }

    }

  }

}
