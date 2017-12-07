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
package schema.provider

import cats.instances.either._
import freestyle.cassandra.TestUtils.{EitherM, MatchersUtil, Null}
import java.io.{ByteArrayInputStream, InputStream}
import org.scalacheck.Prop._
import org.scalatest.WordSpec
import org.scalatest.prop.Checkers

class TroySchemaProviderSpec extends WordSpec with MatchersUtil with Checkers {

  import freestyle.cassandra.schema.MetadataArbitraries._

  "schemaDefinition" should {

    "return the keyspace definition for a valid keyspace cql" in {
      check {
        forAll { keyspace: GeneratedKeyspace =>
          val is: InputStream = new ByteArrayInputStream(keyspace.cql.getBytes)
          val fromString      = TroySchemaProvider[EitherM](keyspace.cql).schemaDefinition
          val fromInputStream = TroySchemaProvider[EitherM](Right(is)).schemaDefinition
          (fromString isEqualTo Right(Seq(keyspace.createKeyspace))) &&
          (fromInputStream isEqualTo Right(Seq(keyspace.createKeyspace)))
        }
      }
    }

    "return the keyspace definition for a valid table cql" in {
      check {
        forAll { table: GeneratedTable =>
          TroySchemaProvider[EitherM](table.cql).schemaDefinition isEqualTo Right(
            Seq(table.createTable))
        }
      }
    }

    "return the keyspace definition for a valid keyspace and table cql" in {
      check {
        forAll { keyspaceAndTable: GeneratedKeyspaceAndTable =>
          TroySchemaProvider[EitherM](keyspaceAndTable.cql).schemaDefinition isEqualTo Right(
            Seq(
              keyspaceAndTable.generatedKeyspace.createKeyspace,
              keyspaceAndTable.generatedTable.createTable))
        }
      }
    }

    "return a left for an invalid cql" in {
      TroySchemaProvider[EitherM]("CREATE KEYSPACE WITH replication").schemaDefinition.isLeft shouldBe true
    }

    "return a left for an invalid inputstream" in {
      TroySchemaProvider[EitherM](Left(new RuntimeException("Test Error"))).schemaDefinition.isLeft shouldBe true
    }

  }

}
