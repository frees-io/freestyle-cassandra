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

import com.datastax.driver.core._
import freestyle.cassandra.implicits.ListenableFutureHandler
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}

import scala.collection.JavaConverters._

class ListenableFutureHandlerSpec
    extends WordSpec
    with Matchers
    with OneInstancePerTest
    with MockFactory
    with TestUtils {

  val sessionMock: Session           = mock[Session]
  val regStMock: RegularStatement    = stub[RegularStatement]
  val prepStMock: PreparedStatement  = stub[PreparedStatement]
  val queryString: String            = "SELECT * FROM table;"
  val mapValues: Map[String, AnyRef] = Map("param1" -> "value1", "param2" -> "value2")

  val handler = new ListenableFutureHandler()(sessionMock)

  "ListenableFutureHandler" should {

    "call to initAsync when calling init() method" in {
      val result = successfulFuture(sessionMock)
      (sessionMock.initAsync _).expects().returns(result)
      handler.init shouldBe result
    }

    "call to closeAsync when calling close() method" in {
      (sessionMock.closeAsync _).expects().returns(CloseFutureTest)
      handler.close
    }

    "call to prepareAsync(String) when calling prepare(String) method" in {
      val result = successfulFuture(prepStMock)
      (sessionMock
        .prepareAsync(_: String))
        .expects(queryString)
        .returns(result)
      handler.prepare(queryString) shouldBe result
    }

    "call to prepareAsync(RegularStatement) when calling prepare(RegularStatement) method" in {
      val result = successfulFuture(prepStMock)
      (sessionMock.prepareAsync(_: RegularStatement)).expects(regStMock).returns(result)
      handler.prepareStatement(regStMock) shouldBe result
    }

    "call to executeAsync(String) when calling execute(String) method" in {
      (sessionMock.executeAsync(_: String)).expects(queryString).returns(ResultSetFutureTest)
      handler.execute(queryString) shouldBe ResultSetFutureTest
    }

    "call to executeAsync(String, java.util.Map) when calling executeWithMap(String, Map) method" in {
      (sessionMock
        .executeAsync(_: String, _: java.util.Map[String, AnyRef]))
        .expects(where { (s, m) =>
          s == queryString && m.asScala == mapValues
        })
        .returns(ResultSetFutureTest)
      handler.executeWithMap(queryString, mapValues) shouldBe ResultSetFutureTest
    }

    "call to executeAsync(Statement) when calling executeStatement(Statement) method" in {
      (sessionMock.executeAsync(_: Statement)).expects(regStMock).returns(ResultSetFutureTest)
      handler.executeStatement(regStMock) shouldBe ResultSetFutureTest
    }

  }

}
