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
package parser

import freestyle.cassandra.schema.model._
import org.scalacheck.{Arbitrary, Gen}
import Arbitrary._

trait KeyspaceArbitraries {

  val keySpaceSimpleReplicationGen: Gen[SimpleStrategyReplication] =
    Gen.posNum[Int] map SimpleStrategyReplication

  val keySpaceNetworkReplicationGen: Gen[NetworkTopologyStrategyReplication] = {

    val tupleGen: Gen[(String, Int)] = for {
      id <- Gen.identifier
      n  <- Gen.posNum[Int]
    } yield (id, n)

    Gen.nonEmptyListOf(tupleGen) map (l => NetworkTopologyStrategyReplication(l.toMap))
  }

  def sqlGen(keyspace: Keyspace): Gen[String] = {

    val replication: String = keyspace.replication match {
      case SimpleStrategyReplication(n) =>
        s"replication = {'class': 'SimpleStrategy', 'replication_factor' : $n}"
      case NetworkTopologyStrategyReplication(map) =>
        val dcJsonArgs = map.toList.map(t => s"'${t._1}': ${t._2}")
        "replication = {'class': 'NetworkTopologyStrategy', %s}".format(dcJsonArgs.mkString(","))
    }

    val durableWrites: Option[String] = keyspace.durableWrites map (dw => s"durable_writes = $dw")

    for {
      ifNotExists      <- arbitrary[Boolean]
      quotedName       <- arbitrary[Boolean]
      replicationFirst <- arbitrary[Boolean]
    } yield {
      val keySpace = "CREATE KEYSPACE " +
        (if (ifNotExists) "IF NOT EXISTS " else "") +
        (if (quotedName) "\"" + keyspace.name + "\"" else keyspace.name)
      val withClause = durableWrites match {
        case Some(dw) if !replicationFirst => "WITH " + dw
        case _                             => "WITH " + replication
      }

      val andClause = (replicationFirst, durableWrites) match {
        case (true, Some(dw)) => "AND " + dw
        case (false, _)       => "AND " + replication
        case _                => ""
      }

      keySpace + " " + withClause + " " + andClause + ";"
    }
  }

  implicit val keySpaceArbitrary: Arbitrary[(Keyspace, String)] = Arbitrary {
    for {
      keyspaceName  <- Gen.identifier.filter(_.length <= 48)
      strategy      <- Gen.oneOf(keySpaceSimpleReplicationGen, keySpaceNetworkReplicationGen)
      durableWrites <- Gen.option(arbitrary[Boolean])
      keySpace = Keyspace(keyspaceName, strategy, durableWrites)
      sql <- sqlGen(keySpace)
    } yield (keySpace, sql)

  }

}
