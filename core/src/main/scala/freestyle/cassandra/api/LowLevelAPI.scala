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

import collection.JavaConverters._
import com.datastax.driver.core._
import freestyle.async._

class LowLevelAPI[F[_]: AsyncM](guavaUtils: GuavaUtils[F]) {

  import guavaUtils._

  def init(implicit session: Session): FreeS[F, Session] =
    call[Session](session.initAsync())

  def close(implicit session: Session): FreeS[F, Unit] =
    call[Void, Unit](session.closeAsync(), _ => (): Unit)

  def prepare(query: String)(implicit session: Session): FreeS[F, PreparedStatement] =
    call[PreparedStatement](session.prepareAsync(query))

  def prepare(statement: RegularStatement)(
      implicit session: Session): FreeS[F, PreparedStatement] =
    call[PreparedStatement](session.prepareAsync(statement))

  def execute(query: String)(implicit session: Session): FreeS[F, ResultSet] =
    call[ResultSet](session.executeAsync(query))

  def execute(statement: Statement)(implicit session: Session): FreeS[F, ResultSet] =
    call[ResultSet](session.executeAsync(statement))

  def execute(query: String, values: Any*)(implicit session: Session): FreeS[F, ResultSet] =
    call[ResultSet](session.executeAsync(query, values))

  def execute(query: String, values: Map[String, AnyRef])(
      implicit session: Session): FreeS[F, ResultSet] =
    call[ResultSet](session.executeAsync(query, values.asJava))

}