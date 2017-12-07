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

package freestyle.free.cassandra
package query.interpolator

import cats.MonadError
import com.datastax.driver.core.{ProtocolVersion, TypeCodec}
import org.scalatest.{Matchers, WordSpec}

import scala.util.Try

class RuntimeCQLInterpolatorSpec extends WordSpec with Matchers {

  "RuntimeCQLInterpolator interpolator" should {

    "return a success for a simple query" in {

      import RuntimeCQLInterpolator._

      cql"SELECT * FROM users" shouldBe (("SELECT * FROM users", Nil))
    }

    "return a success for a query with params" in {

      import RuntimeCQLInterpolator._
      implicit val E: MonadError[Try, Throwable] = cats.instances.try_.catsStdInstancesForTry

      implicit val protocolVersion: ProtocolVersion   = ProtocolVersion.V4
      implicit val stringTypeCodec: TypeCodec[String] = TypeCodec.ascii()
      import freestyle.free.cassandra.codecs._
      val stringCodec: ByteBufferCodec[String] = implicitly[ByteBufferCodec[String]]

      val id: Int      = 1
      val name: String = "username"

      val (query, values) =
        cql"SELECT * FROM users WHERE id = $id AND name = $name"
      query shouldBe "SELECT * FROM users WHERE id = ? AND name = ?"
      values.size shouldBe 2
      values.head.position shouldBe 0
      values.head.serializableValue.serialize shouldBe intCodec.serialize(id)
      values(1).position shouldBe 1
      values(1).serializableValue.serialize shouldBe stringCodec.serialize(name)
    }

    "not compile for a wrong statement" in {

      import RuntimeCQLInterpolator._
      implicit val E: MonadError[Try, Throwable] = cats.instances.try_.catsStdInstancesForTry

      """cql"Wrong statement"""" shouldNot compile
    }

  }

}
