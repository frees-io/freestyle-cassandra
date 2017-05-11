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
package model

import com.datastax.driver.core.{BoundStatement, Session}
import freestyle.cassandra.codecs.ByteBufferCodec

abstract class BaseTable {

  def setValue[T](st: BoundStatement, n: String, v: T)(
      implicit codec: ByteBufferCodec[T]): BoundStatement =
    st.setBytesUnsafe(n, codec.serialize(v))

}

abstract class TableClass1[T1] extends BaseTable {

  def boundedInsert(implicit session: Session, codec: ByteBufferCodec[T1]): BoundStatement

}

abstract class TableClass2[T1, T2] extends BaseTable {

  def boundedInsert(
      implicit session: Session,
      codec1: ByteBufferCodec[T1],
      codec2: ByteBufferCodec[T2]): BoundStatement

}

abstract class TableClass3[T1, T2, T3] extends BaseTable {

  def boundedInsert(
      implicit session: Session,
      codec1: ByteBufferCodec[T1],
      codec2: ByteBufferCodec[T2],
      codec3: ByteBufferCodec[T3]): BoundStatement

}
