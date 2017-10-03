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

import java.nio.ByteBuffer

import com.datastax.driver.core._
import freestyle._
import freestyle.cassandra.codecs.ByteBufferCodec

@free
trait StatementAPI {
  def bind(preparedStatement: PreparedStatement): FS[BoundStatement]

  def setBytesUnsafeByIndex(
      boundStatement: BoundStatement,
      index: Int,
      bytes: ByteBuffer): FS[BoundStatement]

  def setBytesUnsafeByName(
      boundStatement: BoundStatement,
      name: String,
      bytes: ByteBuffer): FS[BoundStatement]

  def setValueByIndex[T](
      boundStatement: BoundStatement,
      index: Int,
      value: T,
      codec: ByteBufferCodec[T]): FS[BoundStatement]

  def setValueByName[T](
      boundStatement: BoundStatement,
      name: String,
      value: T,
      codec: ByteBufferCodec[T]): FS[BoundStatement]
}
