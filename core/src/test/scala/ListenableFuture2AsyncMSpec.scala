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

import cats.~>
import com.google.common.util.concurrent.ListenableFuture
import freestyle.cassandra.TestUtils._
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class ListenableFuture2AsyncMSpec
    extends WordSpec
    with Matchers
    with OneInstancePerTest
    with MockFactory {

  import freestyle.async.implicits._
  import freestyle.asyncGuava.implicits._

  val handler: ListenableFuture ~> Future = listenableFuture2Async[Future]

  "ListenableFuture2AsyncM" should {

    "return a successfully future when a successfully listenable future is passed" in {
      val value = "Hello World!"
      runF(handler(successfulFuture(value))) shouldEqual value
    }

    "return a failed future when a failed listenable future is passed" in {
      val value = "Hello World!"
      runFFailed(handler(failedFuture[String])) shouldBe exception
    }

  }

}
