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
package query

import org.scalatest.WordSpec
import StatementGenerator._
import freestyle.cassandra.TestUtils.MatchersUtil
import org.scalacheck.Prop._
import org.scalatest.prop.Checkers

class StatementGeneratorSpec
    extends WordSpec
    with MatchersUtil
    with Checkers
    with QueryArbitraries {

  case class A(a1: Int, a2: String, a3: Boolean)

  case class B(b1: Long, b2: String)

  case class C(c1: String, c2: B)

  "insert" should {

    "generate a right statement for a regular case class" in {

      import FieldLister._
      check {
        forAll { printer: Printer =>
          implicit val _ = printer
          import printer._
          StatementGenerator[A].insert("A") isEqualTo s"INSERT INTO A (${print("a1")},${print("a2")},${print("a3")}) VALUES (?,?,?)"
        }
      }

    }

    "generate a right statement for a case class with another embedded case class" in {

      import FieldLister._
      check {
        forAll { printer: Printer =>
          implicit val _ = printer
          import printer._
          StatementGenerator[C].insert("C") isEqualTo s"INSERT INTO C (${print("c1")},${print("c2")}) VALUES (?,?)"
        }
      }

    }

    "generate a right expanded statement for a case class with another embedded case class" in {

      import StatementGenerator._
      import FieldListerExpanded._
      check {
        forAll { printer: Printer =>
          implicit val _ = printer
          import printer._
          StatementGenerator[C].insert("C") isEqualTo s"INSERT INTO C (${print("c1")},${print("b1")},${print("b2")}) VALUES (?,?,?)"
        }
      }

    }

  }

  "select" should {

    "generate a right statement for a regular case class" in {

      import FieldLister._
      check {
        forAll { printer: Printer =>
          implicit val _ = printer
          import printer._
          StatementGenerator[A].select("A") isEqualTo s"SELECT ${print("a1")},${print("a2")},${print("a3")} FROM A"
        }
      }

    }

    "generate a right statement for a case class with another embedded case class" in {

      import FieldLister._
      check {
        forAll { printer: Printer =>
          implicit val _ = printer
          import printer._
          StatementGenerator[C].select("C") isEqualTo s"SELECT ${print("c1")},${print("c2")} FROM C"
        }
      }

    }

    "generate a right expanded statement for a case class with another embedded case class" in {

      import FieldListerExpanded._
      check {
        forAll { printer: Printer =>
          implicit val _ = printer
          import printer._
          StatementGenerator[C].select("C") isEqualTo s"SELECT ${print("c1")},${print("b1")},${print("b2")} FROM C"
        }
      }

    }

  }

}
