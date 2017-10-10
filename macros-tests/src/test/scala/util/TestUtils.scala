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
package util

import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

object TestUtils {

  final class TryOps[T](tryValue: Try[T]) {

    val logger: Logger = LoggerFactory.getLogger("freestyle.cassandra.tests.TryOps")

    def logError: Try[T] = {
      tryValue.failed.foreach(e => logger.error("Error on Try", e))
      tryValue
    }

  }

  implicit def tryOps[T](tryValue: Try[T]): TryOps[T] = new TryOps[T](tryValue)

}
