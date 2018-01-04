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
package api

import cats.instances.try_._
import freestyle.cassandra.TestUtils._
import org.scalacheck.Prop._
import org.scalatest.WordSpec
import org.scalatest.prop.Checkers

import scala.util.{Success, Try}

class PackageAPISpec extends WordSpec with MatchersUtil with Checkers {

  def sample(s: String): Try[Int] = Success(s.length)

  "kleisli" should {

    "apply the Kleisli function for a valid dependency" in {

      check {
        forAll { s: String => kleisli(sample).run(s) isEqualTo Success(s.length)
        }
      }

    }

    "return an error for a null dependency" in {
      kleisli(sample).run(Null[String]).isFailure shouldBe true
    }

  }

}
