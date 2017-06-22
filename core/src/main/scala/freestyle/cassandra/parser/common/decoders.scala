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

package freestyle.cassandra.parser.common

import cats.free.Free
import cats.implicits._
import io.circe.Decoder.Result
import io.circe.{Decoder, DecodingFailure, HCursor}

object decoders {

  import freestyle.cassandra.parser.common.model._

  val classField: String             = "class"
  val replicationFactorField: String = "replication_factor"

  private[this] def withClassField[T](field: String)(f: HCursor => Result[T]): Decoder[T] =
    new Decoder[T] {
      override def apply(c: HCursor): Result[T] = {
        c.downField(classField).as[String] flatMap {
          case `field`   => f(c)
          case className => Left(DecodingFailure(s"$className should be '$field'", List.empty))
        }
      }
    }

  val decodeNetworkTopologyStrategy: Decoder[NetworkTopologyStrategyReplication] =
    withClassField(networkTopologyStrategyClass) { c =>
      val dcs: List[String] = (c.fieldSet.getOrElse(Set.empty) - classField).toList
      dcs
        .traverse { field =>
          c.downField(field).as[Int].map(field -> _)
        }
        .map { (tupleList: List[(String, Int)]) =>
          NetworkTopologyStrategyReplication(tupleList.toMap)
        }

    }

  val decodeSimpleStrategy: Decoder[SimpleStrategyReplication] =
    withClassField(simpleStrategyClass) { c =>
      c.downField(replicationFactorField).as[Int].map(SimpleStrategyReplication)
    }
}
