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
package query

import org.scalatest.{Matchers, WordSpec}
import StatementGenerator._

class StatementGeneratorSpec extends WordSpec with Matchers {

  case class A(a1: Int, a2: String, a3: Boolean)

  case class B(b1: Long, b2: String)

  case class C(c1: String, c2: B)

  "insert" should {

    "generate a right statement for a regular case class" in {

      import FieldLister._
      StatementGenerator[A].insert("A") shouldBe "INSERT INTO A (a1,a2,a3) VALUES (?,?,?)"

    }

    "generate a right statement for a case class with another embedded case class" in {

      import FieldLister._
      StatementGenerator[C].insert("C") shouldBe "INSERT INTO C (c1,c2) VALUES (?,?)"

    }

    "generate a right expanded statement for a case class with another embedded case class" in {

      import StatementGenerator._
      import FieldListerExpanded._
      StatementGenerator[C].insert("C") shouldBe "INSERT INTO C (c1,b1,b2) VALUES (?,?,?)"

    }

  }

  "select" should {

    "generate a right statement for a regular case class" in {

      import FieldLister._
      StatementGenerator[A].select("A") shouldBe "SELECT a1,a2,a3 FROM A"

    }

    "generate a right statement for a case class with another embedded case class" in {

      import FieldLister._
      StatementGenerator[C].select("C") shouldBe "SELECT c1,c2 FROM C"

    }

    "generate a right expanded statement for a case class with another embedded case class" in {

      import FieldListerExpanded._
      StatementGenerator[C].select("C") shouldBe "SELECT c1,b1,b2 FROM C"

    }

  }

}
