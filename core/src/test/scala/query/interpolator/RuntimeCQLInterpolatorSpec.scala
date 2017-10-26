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
package query.interpolator

import cats.MonadError
import com.datastax.driver.core.{ProtocolVersion, TypeCodec}
import freestyle.cassandra.query.model.ExecutableStatement
import org.scalatest.{Matchers, WordSpec}

import scala.util.{Success, Try}

class RuntimeCQLInterpolatorSpec extends WordSpec with Matchers {

  "RuntimeCQLInterpolator interpolator" should {

    "return a success for a simple query" in {

      import RuntimeCQLInterpolator._
      import cats.instances.try_._

      val statement: ExecutableStatement = cql"SELECT * FROM users"

      val result = statement.attempt[Try]
      result.isSuccess shouldBe true
      result.get._1 shouldBe "SELECT * FROM users"
      result.get._3.isEmpty shouldBe true
    }

    "return a success for a query with params" in {

      import RuntimeCQLInterpolator._
      implicit val E: MonadError[Try, Throwable] = cats.instances.try_.catsStdInstancesForTry

      implicit val protocolVersion: ProtocolVersion   = ProtocolVersion.V4
      implicit val stringTypeCodec: TypeCodec[String] = TypeCodec.ascii()
      import freestyle.cassandra.codecs._
      val stringCodec: ByteBufferCodec[String] = implicitly[ByteBufferCodec[String]]

      val id: Int      = 1
      val name: String = "username"

      val statement: ExecutableStatement = cql"SELECT * FROM users WHERE id = $id AND name = $name"

      val result = statement.attempt[Try]
      result.isSuccess shouldBe true
      result.get._1 shouldBe "SELECT * FROM users WHERE id = ? AND name = ?"
      result.get._3.size shouldBe 2
      result.get._3.head.position shouldBe 0
      result.get._3.head.serializableValue.serialize shouldBe intCodec.serialize(id)
      result.get._3(1).position shouldBe 1
      result.get._3(1).serializableValue.serialize shouldBe stringCodec.serialize(name)
    }

    "not compile for a wrong statement" in {

      import RuntimeCQLInterpolator._
      implicit val E: MonadError[Try, Throwable] = cats.instances.try_.catsStdInstancesForTry

      """cql"Wrong statement"""" shouldNot compile
    }

  }

}
