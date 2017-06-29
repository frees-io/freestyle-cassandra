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

import com.google.common.util.concurrent.ListenableFuture

object CloseFutureTest extends CloseFuture {
  override def force(): CloseFuture = this

  override def cancel(mayInterruptIfRunning: Boolean): Boolean = true

  override def setFuture(future: ListenableFuture[_ <: Void]): Boolean = true

  override def interruptTask(): Unit = {}

  override def get(timeout: Long, unit: TimeUnit): Void = null

  override def get(): Void = null

  override def setException(throwable: Throwable): Boolean = true

  override def addListener(listener: Runnable, executor: Executor): Unit =
    listener.run()
  override def isCancelled: Boolean = false

  override def set(value: Void): Boolean = true

  override def isDone: Boolean = true
}

case class ResultSetFutureTest(rs: ResultSet) extends ResultSetFuture {
  override def cancel(mayInterruptIfRunning: Boolean): Boolean = true

  override def getUninterruptibly: ResultSet = rs

  override def getUninterruptibly(timeout: Long, unit: TimeUnit): ResultSet = rs

  override def addListener(listener: Runnable, executor: Executor): Unit =
    listener.run()

  override def isCancelled: Boolean = false

  override def isDone: Boolean = true

  override def get(): ResultSet = rs

  override def get(timeout: Long, unit: TimeUnit): ResultSet = rs
}

class StatementTest extends Statement {
  override def getRoutingKey(
      protocolVersion: ProtocolVersion,
      codecRegistry: CodecRegistry): ByteBuffer = null

  override def getKeyspace: String = null
}
