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

package com.datastax.driver.core
import java.nio.ByteBuffer
import java.util.concurrent.{Executor, TimeUnit}

object CloseFutureTest extends CloseFuture {
  override def force(): CloseFuture = this
}

object ResultSetFutureTest extends ResultSetFuture {
  override def cancel(mayInterruptIfRunning: Boolean): Boolean = true

  override def getUninterruptibly: ResultSet = null

  override def getUninterruptibly(timeout: Long, unit: TimeUnit): ResultSet = null

  override def addListener(listener: Runnable, executor: Executor): Unit = (): Unit

  override def isCancelled: Boolean = false

  override def isDone: Boolean = false

  override def get(): ResultSet = null

  override def get(timeout: Long, unit: TimeUnit): ResultSet = null
}

class StatementTest extends Statement {
  override def getRoutingKey(
      protocolVersion: ProtocolVersion,
      codecRegistry: CodecRegistry): ByteBuffer = null

  override def getKeyspace: String = null
}
