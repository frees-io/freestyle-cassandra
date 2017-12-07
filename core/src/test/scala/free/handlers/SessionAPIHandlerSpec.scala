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

package freestyle.free.cassandra
package handlers

import com.datastax.driver.core._
import freestyle.free.cassandra.TestUtils.MatchersUtil
import freestyle.free.cassandra.config.ConfigArbitraries._
import freestyle.free.cassandra.query.QueryArbitraries._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalacheck.Prop._
import org.scalatest.prop.Checkers
import org.scalatest.{OneInstancePerTest, WordSpec}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class SessionAPIHandlerSpec
    extends WordSpec
    with MatchersUtil
    with Checkers
    with OneInstancePerTest
    with MockFactory {

  val sessionMock: Session          = mock[Session]
  val regStMock: RegularStatement   = stub[RegularStatement]
  val prepStMock: PreparedStatement = stub[PreparedStatement]
  val rsMock: ResultSet             = stub[ResultSet]

  import cats.instances.future._
  import freestyle.free.cassandra.implicits._
  import TestUtils._
  val handler: SessionAPIHandler[Future] = new SessionAPIHandler[Future]

  "SessionAPIHandler" should {

    "call to initAsync when calling init() method" in {
      val result = successfulFuture(sessionMock)
      (sessionMock.initAsync _).expects().returns(result)
      runK(handler.init, sessionMock) shouldBe sessionMock
    }

    "call to closeAsync when calling close() method" in {
      (sessionMock.closeAsync _).expects().returns(CloseFutureTest)
      runK(handler.close, sessionMock) shouldBe ((): Unit)
    }

    "call to prepareAsync(String) when calling prepare(String) method" in {
      check {
        forAll(selectQueryGen) { query =>
          val session = mock[Session]
          (session
            .prepareAsync(_: String))
            .expects(query)
            .returns(successfulFuture(prepStMock))
          runK(handler.prepare(query), session) isEqualTo prepStMock
        }
      }
    }

    "call to prepareAsync(RegularStatement) when calling prepare(RegularStatement) method" in {
      val result = successfulFuture(prepStMock)
      (sessionMock.prepareAsync(_: RegularStatement)).expects(regStMock).returns(result)
      runK(handler.prepareStatement(regStMock), sessionMock) shouldBe prepStMock
    }

    "call to executeAsync(String) when calling execute(String) method" in {
      check {
        forAll(selectQueryGen) { query =>
          val session = mock[Session]
          (session
            .executeAsync(_: String))
            .expects(query)
            .returns(ResultSetFutureTest(rsMock))
          runK(handler.execute(query), session) isEqualTo rsMock
        }
      }
    }

    "call to executeAsync(String, java.util.Map) when calling executeWithMap(String, Map) method" in {
      check {
        forAll(selectQueryGen, dataGen) {
          case (query, values) =>
            val session = mock[Session]

            (session
              .executeAsync(_: String, _: java.util.Map[String, AnyRef]))
              .expects {
                where((s, m) => s == query && m.asScala == values)
              }
              .returns(ResultSetFutureTest(rsMock))
            runK(handler.executeWithMap(query, values), session) isEqualTo rsMock
        }
      }
    }

    "call to executeAsync(Statement) when calling executeStatement(Statement) method" in {
      (sessionMock
        .executeAsync(_: Statement))
        .expects(regStMock)
        .returns(ResultSetFutureTest(rsMock))
      runK(handler.executeStatement(regStMock), sessionMock) shouldBe rsMock
    }

    "call to serializableValue and executeAsync(Statement) when calling executeWithByteBuffer method" in {

      check {
        forAll(
          Gen.option(consistencyLevelArb.arbitrary),
          serializableValueByIntListArb.arbitrary,
          selectQueryGen) {
          case (cl, values, query) =>
            val session = mock[Session]

            (session
              .executeAsync(_: Statement))
              .expects(where { (st: Statement) =>
                st.isInstanceOf[SimpleStatement] &&
                  st.asInstanceOf[SimpleStatement].getQueryString() == query &&
                  (st
                    .asInstanceOf[SimpleStatement]
                    .getValues(Null[ProtocolVersion], Null[CodecRegistry])
                    .toList == values.map(_._1)) &&
                  cl.forall(_ == st.getConsistencyLevel)
              })
              .returns(ResultSetFutureTest(rsMock))
            runK(handler.executeWithByteBuffer(query, values.map(_._2), cl), session) isEqualTo rsMock
        }
      }
    }

  }

}
