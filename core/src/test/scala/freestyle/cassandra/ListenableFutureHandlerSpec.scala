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

  val sessionMock: Session           = stub[Session]
  val regStMock: RegularStatement    = stub[RegularStatement]
  val queryString: String            = "SELECT * FROM table;"
  val mapValues: Map[String, AnyRef] = Map("param1" -> "value1", "param2" -> "value2")

  val handler = new ListenableFutureHandler()(sessionMock)

  "ListenableFutureHandler" should {

    "call to initAsync when calling init() method" in {
      handler.init
      (sessionMock.initAsync _).verify()
    }

    "call to closeAsync when calling close() method" in {
      handler.close
      (sessionMock.closeAsync _).verify()
    }

    "call to prepareAsync(String) when calling prepare(String) method" in {
      handler.prepare(queryString)
      (sessionMock.prepareAsync(_: String)).verify(queryString)
    }

    "call to prepareAsync(RegularStatement) when calling prepare(RegularStatement) method" in {
      handler.prepareStatement(regStMock)
      (sessionMock.prepareAsync(_: RegularStatement)).verify(regStMock)
    }

    "call to executeAsync(String) when calling execute(String) method" in {
      handler.execute(queryString)
      (sessionMock.executeAsync(_: String)).verify(queryString)
    }

    "call to executeAsync(String, java.util.Map) when calling executeWithMap(String, Map) method" in {
      handler.executeWithMap(queryString, mapValues)
      (sessionMock
        .executeAsync(_: String, _: java.util.Map[String, AnyRef]))
        .verify(where { (s, m) =>
          s == queryString && m.asScala == mapValues
        })
    }

    "call to executeAsync(Statement) when calling executeStatement(Statement) method" in {
      handler.executeStatement(regStMock)
      (sessionMock.executeAsync(_: Statement)).verify(regStMock)
    }

  }

}
