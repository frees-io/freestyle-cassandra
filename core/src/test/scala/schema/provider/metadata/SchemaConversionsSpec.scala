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
package schema.provider.metadata

import cats.instances.either._
import com.datastax.driver.core._
import freestyle.cassandra.TestUtils._
import org.scalacheck.Prop.forAll
import org.scalamock.scalatest.MockFactory
import org.scalatest.WordSpec
import org.scalatest.prop.Checkers

import scala.collection.JavaConverters._

class SchemaConversionsSpec extends WordSpec with MatchersUtil with MockFactory with Checkers {

  import freestyle.cassandra.schema.MetadataArbitraries._

  object converter extends SchemaConversions

  "toCreateKeyspace" should {

    "return the right keyspace definition for a valid KeyspaceMetadata object" in {
      check {
        forAll { keyspace: GeneratedKeyspace =>
          converter.toCreateKeyspace[EitherM](keyspace.keyspaceMetadata) isEqualTo Right(
            keyspace.createKeyspace)
        }
      }
    }

    "return Left if the name is null" in {
      val metadata: KeyspaceMetadata =
        KeyspaceMetadataTest(Null[String], Map.empty[String, String].asJava)
      converter.toCreateKeyspace(metadata).isLeft shouldBe true
    }

  }

  "toCreateTable" should {

    "return the right table definition for a valid TableMetadata object" in {
      check {
        forAll { table: GeneratedTable =>
          converter.toCreateTable(table.tableMetadata) isEqualTo Right(table.createTable)
        }
      }
    }

  }

  "toCreateIndex" should {

    "return the right index definition for a valid IndexMetadata object" in {
      check {
        forAll { index: GeneratedIndex =>
          converter.toCreateIndex(index.indexMetadata, _ => index.createIndex.tableName) isEqualTo Right(
            index.createIndex)
        }
      }
    }

  }

  "toUserType" should {

    "return the right user type definition for a valid user type object" in {
      check {
        forAll { userType: GeneratedUserType =>
          converter.toUserType(userType.userType) isEqualTo Right(userType.createType)
        }
      }
    }

    "return Left if the field list is null" in {
      val userType: UserType = mock[UserTypeTestDefault]
      (userType.getFieldNames _).expects().returns(Null[java.util.Collection[String]])
      converter.toUserType(userType).isLeft shouldBe true
    }

  }

}
