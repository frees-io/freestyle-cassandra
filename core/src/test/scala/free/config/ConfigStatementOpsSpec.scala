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
package config

import com.datastax.driver.core.{CodecRegistry, PagingState, Statement, StatementTest}
import org.scalacheck.Prop._
import org.scalamock.scalatest.MockFactory
import org.scalatest.prop.Checkers
import org.scalatest.{Matchers, WordSpec}

class ConfigStatementOpsSpec extends WordSpec with Matchers with Checkers with MockFactory {

  import ConfigArbitraries._
  import org.scalacheck.ScalacheckShapeless._
  import config.model._
  import config.model.implicits._
  import scala.collection.JavaConverters._

  "ConfigStatementOps" should {

    "apply all specified values to the statement" in {
      check {
        forAll { cs: ConfigStatement =>
          val st: Statement = mock[StatementTest]
          cs.tracingEnabled foreach {
            case true  => (st.enableTracing _).expects().returns(st)
            case false => (st.disableTracing _).expects().returns(st)
          }
          cs.consistencyLevel foreach (v => (st.setConsistencyLevel _).expects(v).returns(st))
          cs.serialConsistencyLevel foreach (v =>
            (st.setSerialConsistencyLevel _).expects(v).returns(st))
          cs.defaultTimestamp foreach (v => (st.setDefaultTimestamp _).expects(v).returns(st))
          cs.fetchSize foreach (v => (st.setFetchSize _).expects(v).returns(st))
          cs.idempotent foreach (v => (st.setIdempotent _).expects(v).returns(st))
          cs.outgoingPayload foreach (v => (st.setOutgoingPayload _).expects(v.asJava).returns(st))
          cs.pagingState foreach {
            case CodecPagingState(ps, Some(cr)) =>
              (st.setPagingState(_: PagingState, _: CodecRegistry)).expects(ps, cr).returns(st)
            case CodecPagingState(ps, None) =>
              (st.setPagingState(_: PagingState)).expects(ps).returns(st)
            case RawPagingState(array) =>
              (st.setPagingStateUnsafe _).expects(array).returns(st)
          }
          cs.readTimeoutMillis foreach (v => (st.setReadTimeoutMillis _).expects(v).returns(st))
          cs.retryPolicy foreach (v => (st.setRetryPolicy _).expects(v).returns(st))
          cs.applyConf(st) == st
        }
      }
    }
  }

}
