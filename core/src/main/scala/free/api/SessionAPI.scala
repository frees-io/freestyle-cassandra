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

package freestyle.free.cassandra
package api

import com.datastax.driver.core._
import freestyle.free._
import freestyle.free.cassandra.query.model.SerializableValueBy

@free
trait SessionAPI {

  def init: FS[Session]

  def close: FS[Unit]

  def prepare(query: String): FS[PreparedStatement]

  def prepareStatement(statement: RegularStatement): FS[PreparedStatement]

  def execute(query: String): FS[ResultSet]

  def executeWithValues(query: String, values: Any*): FS[ResultSet]

  def executeWithMap(query: String, values: Map[String, AnyRef]): FS[ResultSet]

  def executeStatement(statement: Statement): FS[ResultSet]

  def executeWithByteBuffer(
      query: String,
      values: List[SerializableValueBy[Int]],
      consistencyLevel: Option[ConsistencyLevel] = None): FS[ResultSet]

}
