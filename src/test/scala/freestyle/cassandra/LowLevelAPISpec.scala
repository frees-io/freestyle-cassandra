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

package freestyle
package cassandra

import com.datastax.driver.core._
import freestyle.async.AsyncM
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}

import scala.collection.JavaConverters._

class LowLevelAPISpec
    extends WordSpec
    with Matchers
    with OneInstancePerTest
    with MockFactory
    with TestUtils {

  class GuavaUtilsOp extends GuavaUtils[AsyncM.Op]

  val sessionMock: Session           = stub[Session]
  val regStMock: RegularStatement    = stub[RegularStatement]
  val queryString: String            = "SELECT * FROM table;"
  val values: List[String]           = List("param1", "param2")
  val mapValues: Map[String, AnyRef] = Map("param1" -> "value1", "param2" -> "value2")

  val sessionResult: FreeS[AsyncM.Op, Session]          = FreeS.pure(sessionMock)
  val unitResult: FreeS[AsyncM.Op, Unit]                = FreeS.pure((): Unit)
  val prepStResult: FreeS[AsyncM.Op, PreparedStatement] = FreeS.pure(stub[PreparedStatement])
  val rsResult: FreeS[AsyncM.Op, ResultSet]             = FreeS.pure(stub[ResultSet])

  val guavaUtilsMock: GuavaUtilsOp = mock[GuavaUtilsOp]

  val api = new LowLevelAPI[AsyncM.Op](guavaUtilsMock)

  "LowLevelAPI" should {

    "works as expected when calling init() method" in {
      (guavaUtilsMock.call[Session] _).expects(*).returns(sessionResult)

      api.init(sessionMock) shouldBe sessionResult
      (sessionMock.initAsync _).verify()
    }

    "works as expected when calling close() method" in {
      (guavaUtilsMock.call[Void, Unit] _).expects(*, *).returns(unitResult)

      api.close(sessionMock) shouldBe unitResult
      (sessionMock.closeAsync _).verify()
    }

    "works as expected when calling prepare(String) method" in {
      (guavaUtilsMock.call[PreparedStatement] _).expects(*).returns(prepStResult)

      api.prepare(queryString)(sessionMock) shouldBe prepStResult
      (sessionMock.prepareAsync(_: String)).verify(queryString)
    }

    "works as expected when calling prepare(RegularStatement) method" in {
      (guavaUtilsMock.call[PreparedStatement] _).expects(*).returns(prepStResult)

      api.prepare(regStMock)(sessionMock) shouldBe prepStResult
      (sessionMock.prepareAsync(_: RegularStatement)).verify(regStMock)
    }

    "works as expected when calling execute(String) method" in {
      (guavaUtilsMock.call[ResultSet] _).expects(*).returns(rsResult)

      api.execute(queryString)(sessionMock) shouldBe rsResult
      (sessionMock.executeAsync(_: String)).verify(queryString)
    }

    "works as expected when calling execute(Statement) method" in {
      (guavaUtilsMock.call[ResultSet] _).expects(*).returns(rsResult)

      api.execute(regStMock)(sessionMock) shouldBe rsResult
      (sessionMock.executeAsync(_: Statement)).verify(regStMock)
    }

    "works as expected when calling execute(String, Any*) method" in {
      (guavaUtilsMock.call[ResultSet] _).expects(*).returns(rsResult)
      api.execute(queryString, values: _*)(sessionMock) shouldBe rsResult
    }

    "works as expected when calling execute(String, Map[String, AnyRef]) method" in {
      (guavaUtilsMock.call[ResultSet] _).expects(*).returns(rsResult)
      api.execute(queryString, mapValues)(sessionMock) shouldBe rsResult
      (sessionMock
        .executeAsync(_: String, _: java.util.Map[String, AnyRef]))
        .verify(where { (s, m) =>
          s == queryString && m.asScala == mapValues
        })
    }

  }

}
