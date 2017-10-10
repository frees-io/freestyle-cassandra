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

package freestyle.cassandra.macros
package interpolator

import java.nio.ByteBuffer
import java.util.UUID

import com.datastax.driver.core.{ProtocolVersion, TypeCodec}
import freestyle.cassandra.query.model.SerializableValueBy
import org.scalatest.{Matchers, WordSpec}

import scala.util.{Success, Try}

class SchemaFileInterpolatorTest extends WordSpec with Matchers {

  "SchemaFileInterpolator" should {

    "works as expected for a simple valid query" in {

      import MySchemaInterpolator._
      cql"SELECT * FROM test.users" shouldBe (("SELECT * FROM test.users", Nil))
    }

    "works as expected for a valid query with params" in {

      import MySchemaInterpolator._
      implicit val protocolVersion: ProtocolVersion   = ProtocolVersion.V4
      implicit val stringTypeCodec: TypeCodec[String] = TypeCodec.ascii()
      implicit val uuidTypeCodec: TypeCodec[UUID]     = TypeCodec.uuid()
      import freestyle.cassandra.codecs._

      val id = UUID.randomUUID()

      val expectedCQL: String       = "SELECT id, name FROM test.users WHERE id = ?"
      val expectedValue: ByteBuffer = uuidTypeCodec.serialize(id, protocolVersion)

      val (cql: String, values: List[SerializableValueBy[Int]]) =
        cql"SELECT id, name FROM test.users WHERE id = $id"

      cql shouldBe expectedCQL
      values.size shouldBe 1
      values.head.position shouldBe 0
      values.head.serializableValue
        .serialize[Try](cats.instances.try_.catsStdInstancesForTry) shouldBe Success(expectedValue)
    }

    "doesn't compile for an invalid query" in {

      import MySchemaInterpolator._
      """cql"SELECT * FROM unknownTable"""" shouldNot compile
    }

    "doesn't compile when passing an invalid schema path" in {

      import MyInvalidSchemaInterpolator._
      """cql"SELECT * FROM unknownTable"""" shouldNot compile
    }

  }

}
