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

import freestyle.cassandra.TestUtils.MatchersUtil
import org.scalacheck.Prop._
import org.scalatest.WordSpec
import org.scalatest.prop.Checkers

class FieldListerSpec extends WordSpec with MatchersUtil with Checkers with QueryArbitraries {

  case class A(a1: Int, a2: String, a3: Boolean)

  case class B(b1: Long, b2: String)

  case class C(c1: String, c2: B)

  "FieldLister" should {

    "list the fields for a case class" in {
      check {
        forAll { printer: Printer =>
          implicit val _ = printer
          implicitly[FieldLister[A]].list isEqualTo List("a1", "a2", "a3").map(printer.print)
        }
      }

    }

    "list the fields for a case class with another embedded case class" in {
      check {
        forAll { printer: Printer =>
          implicit val _ = printer
          implicitly[FieldLister[C]].list isEqualTo List("c1", "c2").map(printer.print)
        }
      }

    }

    "list the expanded fields for a case class with another embedded case class" in {
      import FieldListerExpanded._
      check {
        forAll { printer: Printer =>
          implicit val _ = printer
          implicitly[FieldLister[C]].list isEqualTo List("c1", "b1", "b2").map(printer.print)
        }
      }

    }

  }

}
