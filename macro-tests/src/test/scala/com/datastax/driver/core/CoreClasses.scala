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

import com.datastax.driver.core.policies.RetryPolicy
import java.nio.ByteBuffer
import java.util

import scala.collection.mutable

object PreparedStatementTest extends PreparedStatement {

  override def getVariables = ColumnDefinitionsTest

  override def bind(values: AnyRef*): BoundStatement = BoundStatementTest

  override def bind = BoundStatementTest

  override def setRoutingKey(routingKey: ByteBuffer) = null

  override def setRoutingKey(routingKeyComponents: ByteBuffer*) = null

  override def getRoutingKey = null

  override def setConsistencyLevel(consistency: ConsistencyLevel) = null

  override def getConsistencyLevel = null

  override def setSerialConsistencyLevel(serialConsistency: ConsistencyLevel) = null

  override def getSerialConsistencyLevel = null

  override def getQueryString = null

  override def getQueryKeyspace = null

  override def enableTracing = null

  override def disableTracing = null

  override def isTracing = false

  override def setRetryPolicy(policy: RetryPolicy) = null

  override def getRetryPolicy = null

  override def getPreparedId = PrepareIdTest

  override def getIncomingPayload = new util.HashMap[String, ByteBuffer]

  override def getOutgoingPayload = new util.HashMap[String, ByteBuffer]

  override def setOutgoingPayload(payload: util.Map[String, ByteBuffer]) = null

  override def getCodecRegistry = null

  override def setIdempotent(idempotent: java.lang.Boolean) = null

  override def isIdempotent = null
}

class BoundStatementTest extends BoundStatement(PreparedStatementTest) {

  val values: mutable.Map[String, ByteBuffer] = mutable.Map[String, ByteBuffer]()

  override def setOutgoingPayload(payload: util.Map[String, ByteBuffer]) = null

  override def setBytesUnsafe(name: String, v: ByteBuffer): BoundStatement = {
    values += (name -> v)
    this
  }

  override def bind(values: AnyRef*): BoundStatement = this

  def getValues: Map[String, ByteBuffer] = values.toMap
}

object BoundStatementTest extends BoundStatementTest

object ColumnDefinitionsTest
    extends ColumnDefinitions(
      Array.empty[ColumnDefinitions.Definition],
      CodecRegistry.DEFAULT_INSTANCE)

object PrepareIdTest
    extends PreparedId(
      MD5Digest.wrap(Array.empty[Byte]),
      ColumnDefinitionsTest,
      ColumnDefinitionsTest,
      Array.empty[Int],
      ProtocolVersion.V5) {}
