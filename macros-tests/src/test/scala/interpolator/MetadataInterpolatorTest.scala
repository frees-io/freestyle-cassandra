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

package freestyle.cassandra.macros
package interpolator

import freestyle.cassandra.util.CassandraUtil
import com.datastax.driver.core.Cluster
import org.apache.cassandra.service.CassandraDaemon
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration._

class MetadataInterpolatorTest extends WordSpec with Matchers with BeforeAndAfterAll {

  import freestyle.cassandra.util.CassandraConfigurationValues._
  import freestyle.cassandra.util.TestUtils._

  var daemon: Option[CassandraDaemon] = None

  override def beforeAll: Unit = {

    CassandraUtil.setLogLevelToWarn().logError

    CassandraUtil.startCassandra().logError.foreach { future =>
      daemon = Option(Await.result(future, 60.seconds))
      CassandraUtil.executeCQL("/schema.sql").logError
    }
  }

  override protected def afterAll(): Unit =
    daemon.foreach { d =>
      CassandraUtil.stopCassandra(d).logError.foreach(Await.result(_, 60.seconds))
    }

  "MetadataInterpolator" should {

    "works" in {
      val cluster = new Cluster.Builder()
        .withClusterName(clusterName)
        .addContactPoint(listenAddress)
        .withPort(nativePort)
        .build()
      val session = cluster.connect()
      val rs      = session.execute("SELECT * FROM test.users")
      Option(rs.one()) shouldBe None
      cluster.close()
    }

  }
}
