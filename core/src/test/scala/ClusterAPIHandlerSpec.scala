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
import com.google.common.util.concurrent.ListenableFuture
import freestyle.cassandra.api.{ClusterAPIOps, LowLevelAPIOps}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

class ClusterAPIHandlerSpec
    extends WordSpec
    with Matchers
    with OneInstancePerTest
    with MockFactory
    with TestUtils {

  val clusterMock: Cluster         = mock[Cluster]
  val sessionMock: Session         = mock[Session]
  val keyspace: String             = "keyspace"
  val configuration: Configuration = new Configuration.Builder().build()

  import cats.instances.future._
  import freestyle.async.implicits._
  import freestyle.cassandra.implicits._
  val handler: ClusterAPIHandler[Future] = clusterAPIHandler[Future]

  import scala.concurrent.duration._
  def run[T](k: ClusterAPIOps[Future, T]): T =
    Await.result(k.run(clusterMock), 5.seconds)

  "ListenableFutureHandler" should {

    "call to connectAsync when calling connect() method" in {
      val result = successfulFuture(sessionMock)
      (clusterMock.connectAsync _: () => ListenableFuture[Session]).expects().returns(result)
      run(handler.connect) shouldBe sessionMock
    }

    "call to connectAsync when calling connectKeyspace(String) method" in {
      val result = successfulFuture(sessionMock)
      (clusterMock.connectAsync(_: String)).expects(keyspace).returns(result)
      run(handler.connectKeyspace(keyspace)) shouldBe sessionMock
    }

    "call to closeAsync when calling close() method" in {
      (clusterMock.closeAsync _).expects().returns(CloseFutureTest)
      run(handler.close) shouldBe ((): Unit)
    }

    "call to getConfiguration when calling configuration method" in {
      (clusterMock.getConfiguration _)
        .expects()
        .returns(configuration)
      run(handler.configuration) shouldBe configuration
    }

    "throw the exception when calling configuration method" in {
      (clusterMock.getConfiguration _)
        .expects()
        .throws(new RuntimeException(""))
      intercept[RuntimeException](run(handler.configuration))
    }

    "call to getMetadata when calling metadata method" in {
      (clusterMock.getMetadata _)
        .expects()
        .returns(MetadataTest)
      run(handler.metadata) shouldBe MetadataTest
    }

    "throw the exception when calling metadata method" in {
      (clusterMock.getMetadata _)
        .expects()
        .throws(new RuntimeException(""))
      intercept[RuntimeException](run(handler.metadata))
    }

    "call to getMetrics when calling metrics method" in {
      (clusterMock.getMetrics _)
        .expects()
        .returns(MetricsTest)
      run(handler.metrics) shouldBe MetricsTest
    }

    "throw the exception when calling metrics method" in {
      (clusterMock.getMetrics _)
        .expects()
        .throws(new RuntimeException(""))
      intercept[RuntimeException](run(handler.metrics))
    }

  }

}
