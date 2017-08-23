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
package schema.validator

import cats.data.Validated.Valid
import freestyle.cassandra.TestUtils.MatchersUtil
import freestyle.cassandra.schema.{SchemaDefinition, SchemaDefinitionProviderError}
import freestyle.cassandra.schema.provider.SchemaDefinitionProvider
import org.scalacheck.Prop.forAll
import org.scalatest.WordSpec
import org.scalatest.prop.Checkers

class TroySchemaValidatorSpec extends WordSpec with MatchersUtil with Checkers {

  import freestyle.cassandra.schema.MetadataArbitraries._
  import TroySchemaValidator._

  "validateStatement" should {

    "work as expected" in {

      check {
        forAll { st: GeneratedStatement =>
          val sdp = new SchemaDefinitionProvider {
            override def schemaDefinition: Either[SchemaDefinitionProviderError, SchemaDefinition] =
              Right(Seq(st.keyspace, st.table))
          }

          (instance.validateStatement(sdp, st.validStatement._2) isEqualTo Valid((): Unit)) &&
          (instance.validateStatement(sdp, st.invalidStatement._2).isInvalid isEqualTo true)
        }
      }

    }

  }

}
