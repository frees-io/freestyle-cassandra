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
package api

import cats.{~>, Id}
import com.datastax.driver.core._
import freestyle._
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}

class ClusterAPISpec extends WordSpec with Matchers with OneInstancePerTest with MockFactory {

  import TestUtils._

  val sessionMock: Session         = stub[Session]
  val configuration: Configuration = new Configuration.Builder().build()
  val unit: Unit                   = ()
  val keyspace: String             = "keyspace"
  val metadataTest: Metadata       = MetadataTest()

  implicit val clusterAPIHandler: ClusterAPI.Op ~> Id = new (ClusterAPI.Op ~> Id) {
    override def apply[A](fa: ClusterAPI.Op[A]): Id[A] = fa match {
      case ClusterAPI.ConnectOp()          => sessionMock
      case ClusterAPI.ConnectKeyspaceOp(_) => sessionMock
      case ClusterAPI.CloseOp()            => unit
      case ClusterAPI.ConfigurationOp()    => configuration
      case ClusterAPI.MetadataOp()         => metadataTest
      case ClusterAPI.MetricsOp()          => MetricsTest
    }
  }

  "ClusterAPI" should {

    "work as expect when calling OP" in {

      type ReturnResult = (Session, Session, Unit, Configuration, Metadata, Metrics)

      def program[F[_]](implicit api: ClusterAPI[F]): FreeS[F, ReturnResult] = {
        for {
          v1 <- api.connect
          v2 <- api.connectKeyspace(keyspace)
          v3 <- api.close
          v4 <- api.configuration
          v5 <- api.metadata
          v6 <- api.metrics
        } yield (v1, v2, v3, v4, v5, v6)
      }

      val result = program[ClusterAPI.Op].interpret[Id]
      result shouldBe ((sessionMock, sessionMock, unit, configuration, metadataTest, MetricsTest))
    }

  }

}
