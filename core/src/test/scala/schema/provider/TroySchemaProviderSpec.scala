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

import freestyle.cassandra.TestUtils.MatchersUtil
import org.scalacheck.Prop._
import org.scalatest.WordSpec
import org.scalatest.prop.Checkers

class TroySchemaProviderSpec extends WordSpec with MatchersUtil with Checkers {

  import freestyle.cassandra.schema.MetadataArbitraries._

  "schemaDefinition" should {

    "return the keyspace definition for a valid keyspace cql" in {
      check {
        forAll { keyspace: GeneratedKeyspace =>
          TroySchemaProvider(keyspace.cql).schemaDefinition isEqualTo Right(
            Seq(keyspace.createKeyspace))
        }
      }
    }

    "return the keyspace definition for a valid table cql" in {
      check {
        forAll { table: GeneratedTable =>
          TroySchemaProvider(table.cql).schemaDefinition isEqualTo Right(Seq(table.createTable))
        }
      }
    }

    "return the keyspace definition for a valid keyspace and table cql" in {
      check {
        forAll { (keyspace: GeneratedKeyspace, tables: List[GeneratedTable]) =>
          val cql = keyspace.cql ++ tables.map(_.cql).mkString("\n", "\n", "")
          TroySchemaProvider(cql).schemaDefinition isEqualTo Right(
            Seq(keyspace.createKeyspace) ++ tables.map(_.createTable))
        }
      }
    }

    "return a left for an invalid cql" in {
      TroySchemaProvider("CREATE KEYSPACE WITH replication").schemaDefinition.isLeft shouldBe true
    }

  }

}
