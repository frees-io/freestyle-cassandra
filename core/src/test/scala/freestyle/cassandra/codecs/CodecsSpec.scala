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

package freestyle.cassandra.codecs

import java.nio.ByteBuffer

import cats.syntax.either._
import com.datastax.driver.core.exceptions.InvalidTypeException
import com.datastax.driver.core.{ProtocolVersion, TypeCodec}
import freestyle.cassandra.codecs
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Prop._
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.prop.Checkers

class CodecsSpec extends WordSpec with Matchers with Checkers {

  import codecs._

  def checkInverseCodec[T](codec: ByteBufferCodec[T])(implicit A: Arbitrary[T]) =
    check {
      forAll { v: T =>
        codec.deserialize(codec.serialize(v)) == v
      }
    }

  def byteBufferGen[T](codec: ByteBufferCodec[T], defaultValue: T)(
      implicit A: Arbitrary[T]): Gen[(ByteBuffer, T)] = {

    val nullByteBuffer = null.asInstanceOf[ByteBuffer]

    def codecGen: Gen[(ByteBuffer, T)] =
      for {
        value <- A.arbitrary
        bb = codec.serialize(value)
        remaining <- Gen.chooseNum[Int](0, bb.limit())
        _ = bb.position(bb.limit() - remaining)
      } yield (bb, value)

    Gen.oneOf(Gen.const((nullByteBuffer, defaultValue)), codecGen)
  }

  def checkDeserialize[T](codec: ByteBufferCodec[T], byteSize: Int, defaultValue: T)(
      implicit A: Arbitrary[T]) = {
    val prop = forAll(byteBufferGen(codec, defaultValue)) {
      case (bb, v) =>
        val deserialized = Either.catchNonFatal(codec.deserialize(bb))
        if (bb == null || bb.remaining() == 0) {
          deserialized == Right(defaultValue)
        } else if (bb.remaining() == byteSize) {
          deserialized == Right(v)
        } else {
          deserialized.isLeft && deserialized.left.get.isInstanceOf[InvalidTypeException]
        }
    }
    check(prop, minSuccessful(500))
  }

  val consumedByteBuffer: ByteBuffer = {
    val byteBuffer = ByteBuffer.allocate(8)
    byteBuffer.limit(8)
    byteBuffer.position(8)
    byteBuffer
  }

  "Boolean codec" should {

    val codec        = codecs.booleanCodec
    val byteSize     = 1
    val defaultValue = false

    "serialize and deserialize are inverse" in {
      checkInverseCodec(codec)
    }

    "deserialize all possible values" in {
      checkDeserialize(codec, byteSize, defaultValue)
    }
  }

  "Byte codec" should {

    val codec        = codecs.byteCodec
    val byteSize     = 1
    val defaultValue = 0

    "serialize and deserialize are inverse" in {
      checkInverseCodec(codec)
    }

    "deserialize all possible values" in {
      checkDeserialize(codec, byteSize, defaultValue.toByte)
    }
  }

  "Double codec" should {

    val codec        = codecs.doubleCodec
    val byteSize     = 8
    val defaultValue = 0d

    "serialize and deserialize are inverse" in {
      checkInverseCodec(codec)
    }

    "deserialize all possible values" in {
      checkDeserialize(codec, byteSize, defaultValue)
    }
  }

  "Float codec" should {

    val codec        = codecs.floatCodec
    val byteSize     = 4
    val defaultValue = 0f

    "serialize and deserialize are inverse" in {
      checkInverseCodec(codec)
    }

    "deserialize all possible values" in {
      checkDeserialize(codec, byteSize, defaultValue)
    }
  }

  "Int codec" should {

    val codec        = codecs.intCodec
    val byteSize     = 4
    val defaultValue = 0

    "serialize and deserialize are inverse" in {
      checkInverseCodec(codec)
    }

    "deserialize all possible values" in {
      checkDeserialize(codec, byteSize, defaultValue)
    }
  }

  "Long codec" should {

    val codec        = codecs.longCodec
    val byteSize     = 8
    val defaultValue = 0l

    "serialize and deserialize are inverse" in {
      checkInverseCodec(codec)
    }

    "deserialize all possible values" in {
      checkDeserialize(codec, byteSize, defaultValue)
    }
  }

  "Short codec" should {

    val codec        = codecs.shortCodec
    val byteSize     = 2
    val defaultValue = 0

    "serialize and deserialize are inverse" in {
      checkInverseCodec(codec)
    }

    "deserialize all possible values" in {
      checkDeserialize(codec, byteSize, defaultValue.toShort)
    }
  }

  "String codec" should {

    val codec        = codecs.stringCodec
    val defaultValue = ""

    "serialize and deserialize are inverse" in {
      checkInverseCodec(codec)
    }

    "deserialize all possible values" in {
      val prop = forAll(byteBufferGen(codec, defaultValue)(Arbitrary(Gen.alphaStr))) {
        case (bb, v) =>
          val deserialized = Either.catchNonFatal(codec.deserialize(bb))
          val expected = if (bb == null || bb.remaining() == 0) {
            Right(defaultValue)
          } else {
            Right(v.substring(v.length - bb.remaining()))
          }
          deserialized shouldBe expected
          deserialized == expected
      }
      check(prop, minSuccessful(500))
    }
  }

}
