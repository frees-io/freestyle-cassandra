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
package config

import freestyle.cassandra.config.ClusterConfig.{
  PoolingOptionsConfig,
  QueryOptionsConfig,
  SocketOptionsConfig
}
import org.scalacheck.Prop._

import scala.collection.JavaConverters._

class ClusterDecoderSpec extends TestDecoderUtils {

  import ConfigArbitraries._
  import classy.config._
  import com.typesafe.config.Config
  import com.typesafe.config.ConfigFactory
  import org.scalacheck.ScalacheckShapeless._

  "PoolingOptions Decoder" should {

    "parse and set the right values in the PoolingOptions builder" in {
      check {
        forAll { config: PoolingOptionsConfig =>
          val (builder, poc) = preparePoolingOptionsDecoder(config)

          val decoder      = readConfig[Config]("poolingOptions") andThen builder.build
          val configString = s"poolingOptions = ${poc.print}"
          val rawConfig    = ConfigFactory.parseString(configString)
          decoder(rawConfig).isRight
        }
      }
    }
  }

  "QueryOptions Decoder" should {

    "parse and set the right values in the QueryOptions builder" in {
      check {
        forAll { config: QueryOptionsConfig =>
          val (builder, qob) = prepareQueryOptionsDecoder(config)

          val decoder      = readConfig[Config]("queryOptions") andThen builder.build
          val configString = s"queryOptions = ${qob.print}"
          val rawConfig    = ConfigFactory.parseString(configString)
          decoder(rawConfig).isRight
        }
      }
    }
  }

  "SocketOptions Decoder" should {

    "parse and set the right values in the SocketOptions builder" in {
      check {
        forAll { config: SocketOptionsConfig =>
          val (builder, sob) = prepareSocketOptionsDecoder(config)

          val decoder      = readConfig[Config]("socketOptions") andThen builder.build
          val configString = s"socketOptions = ${sob.print}"
          val rawConfig    = ConfigFactory.parseString(configString)
          decoder(rawConfig).isRight
        }
      }
    }
  }

  "ClusterBuilder Decoder" should {

    "parse a valid configuration and set the right values in the Cluster.Builder" in {

      val decoder      = readConfig[Config]("cluster") andThen decoders.clusterBuilderDecoder
      val configString = s"cluster = ${validClusterConfiguration.print}"
      val rawConfig    = ConfigFactory.parseString(configString)
      val result       = decoder(rawConfig)
      result.isRight shouldBe true
      val builder = result.right.get
      Option(builder.getClusterName) shouldBe validClusterConfiguration.name
      builder.getContactPoints.asScala.headOption.map(_.getHostString) shouldBe Some("127.0.0.1")
    }

  }

}
