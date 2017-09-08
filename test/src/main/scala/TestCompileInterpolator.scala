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

import freestyle.cassandra.schema.Statement

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object TestCompileInterpolator extends App {

  import freestyle.cassandra.query.interpolator.MyInterpolator._

  import scala.concurrent.ExecutionContext.Implicits.global
  import cats.instances.future._

  val id = 1

  val r: Future[Statement] = cql"SELECT id, name FROM test.users WHERE id = $id;"
  val st: Statement        = Await.result(r, Duration.Inf)
  println(st)

}
