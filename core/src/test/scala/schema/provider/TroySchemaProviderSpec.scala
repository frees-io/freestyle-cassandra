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

import org.scalatest.{Matchers, WordSpec}

class TroySchemaProviderSpec extends WordSpec with Matchers {

  import freestyle.cassandra.schema.SchemaData._

  "schemaDefinition" should {

    "return the keyspace definition for a valid keyspace cql" in {
      new TroySchemaProvider(keyspaceCQL).schemaDefinition shouldBe Right(Seq(keyspaceDef))
    }

    "return the keyspace definition for a valid table cql" in {
      new TroySchemaProvider(tableCQL).schemaDefinition shouldBe Right(Seq(tableDef))
    }

    "return the keyspace definition for a valid keyspace and table cql" in {
      new TroySchemaProvider(CQL).schemaDefinition shouldBe Right(Seq(keyspaceDef, tableDef))
    }

    "return a left for an invalid cql" in {
      new TroySchemaProvider("CREATE KEYSPACE WITH replication").schemaDefinition.isLeft shouldBe true
    }

  }

}
