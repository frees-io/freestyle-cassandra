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
package handlers

import com.datastax.driver.core._
import freestyle.cassandra.api.SessionAPIOps
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}

import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

class SessionAPIHandlerSpec
    extends WordSpec
    with Matchers
    with OneInstancePerTest
    with MockFactory {

  val sessionMock: Session           = mock[Session]
  val regStMock: RegularStatement    = stub[RegularStatement]
  val prepStMock: PreparedStatement  = stub[PreparedStatement]
  val rsMock: ResultSet              = stub[ResultSet]
  val queryString: String            = "SELECT * FROM table;"
  val mapValues: Map[String, AnyRef] = Map("param1" -> "value1", "param2" -> "value2")
  val values: Seq[Any]               = Seq("value1", "value2")

  import freestyle.async.implicits._
  import freestyle.cassandra.handlers.implicits._
  import TestUtils._
  val handler: SessionAPIHandler[Future] = sessionAPIHandler[Future]

  import scala.concurrent.duration._
  def run[T](k: SessionAPIOps[Future, T]): T =
    Await.result(k.run(sessionMock), 5.seconds)

  "SessionAPIHandler" should {

    "call to initAsync when calling init() method" in {
      val result = successfulFuture(sessionMock)
      (sessionMock.initAsync _).expects().returns(result)
      run(handler.init) shouldBe sessionMock
    }

    "call to closeAsync when calling close() method" in {
      (sessionMock.closeAsync _).expects().returns(CloseFutureTest)
      run(handler.close) shouldBe ((): Unit)
    }

    "call to prepareAsync(String) when calling prepare(String) method" in {
      val result = successfulFuture(prepStMock)
      (sessionMock
        .prepareAsync(_: String))
        .expects(queryString)
        .returns(result)
      run(handler.prepare(queryString)) shouldBe prepStMock
    }

    "call to prepareAsync(RegularStatement) when calling prepare(RegularStatement) method" in {
      val result = successfulFuture(prepStMock)
      (sessionMock.prepareAsync(_: RegularStatement)).expects(regStMock).returns(result)
      run(handler.prepareStatement(regStMock)) shouldBe prepStMock
    }

    "call to executeAsync(String) when calling execute(String) method" in {
      (sessionMock
        .executeAsync(_: String))
        .expects(queryString)
        .returns(ResultSetFutureTest(rsMock))
      run(handler.execute(queryString)) shouldBe rsMock
    }

    "call to executeAsync(String, java.util.Map) when calling executeWithMap(String, Map) method" in {
      (sessionMock
        .executeAsync(_: String, _: java.util.Map[String, AnyRef]))
        .expects(where { (s, m) =>
          s == queryString && m.asScala == mapValues
        })
        .returns(ResultSetFutureTest(rsMock))
      run(handler.executeWithMap(queryString, mapValues)) shouldBe rsMock
    }

    "call to executeAsync(Statement) when calling executeStatement(Statement) method" in {
      (sessionMock
        .executeAsync(_: Statement))
        .expects(regStMock)
        .returns(ResultSetFutureTest(rsMock))
      run(handler.executeStatement(regStMock)) shouldBe rsMock
    }

  }

}
