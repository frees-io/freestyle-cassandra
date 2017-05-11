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

package freestyle
package cassandra
package api

import cats.instances.future._
import freestyle.async.AsyncM
import freestyle.async.implicits._
import org.scalatest.{Assertion, AsyncWordSpec, Matchers}

import scala.concurrent.Future

class GuavaUtilsSpec extends AsyncWordSpec with Matchers with TestUtils {

  val string: String = "Hello World!"
  val sLength: Int   = string.length

  def verifySuccess[F[_]: AsyncM, T](free: FreeS[F, T], expected: T)(
      implicit handler: FSHandler[F, Future]): Future[Assertion] =
    free.interpret[Future] map { _ shouldBe expected }

  def verifyFailure[F[_]: AsyncM, T](free: FreeS[F, T])(
      implicit handler: FSHandler[F, Future]): Future[Assertion] =
    free.interpret[Future] recover { case `exception` => 42 } map { _ shouldBe 42 }

  val guavaUtils = new GuavaUtils[AsyncM.Op]

  "GuavaUtils.call" should {

    "return the right response if the future ends successfully" in {
      verifySuccess(guavaUtils.call(successfulFuture(string)), string)
    }

    "return an exception if the future ends with a failure" in {
      verifyFailure(guavaUtils.call(failedFuture[String]))
    }

  }

  "GuavaUtils.call with map" should {

    "return the right response if the future ends successfully" in {
      verifySuccess(guavaUtils.call[String, Int](successfulFuture(string), _.length), sLength)
    }

    "return an exception if the future ends with a failure" in {
      verifyFailure(guavaUtils.call[String, Int](failedFuture[String], _.length))
    }

  }

}
