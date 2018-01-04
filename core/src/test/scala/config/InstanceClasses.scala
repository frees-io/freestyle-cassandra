/*
 * Copyright 2017-2018 47 Degrees, LLC. <http://www.47deg.com>
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
package config

import java.net.InetSocketAddress
import java.util
import java.util.concurrent.Executor

import com.datastax.driver.core._
import com.datastax.driver.core.exceptions.DriverException
import com.datastax.driver.core.policies._
import io.netty.channel.socket.SocketChannel
import io.netty.handler.ssl.SslHandler

import TestUtils._

class MyJavaExecutor extends Executor {
  override def execute(command: Runnable): Unit = {}
}

class MyAddressTranslator extends AddressTranslator {
  override def init(cluster: Cluster): Unit                             = {}
  override def translate(address: InetSocketAddress): InetSocketAddress = Null[InetSocketAddress]
  override def close(): Unit                                            = {}
}

class MyAuthProvider extends AuthProvider {
  override def newAuthenticator(host: InetSocketAddress, authenticator: String): Authenticator =
    Null[Authenticator]
}

class MyLoadBalancingPolicy extends LoadBalancingPolicy {
  override def newQueryPlan(loggedKeyspace: String, statement: Statement): util.Iterator[Host] =
    Null[util.Iterator[Host]]
  override def init(cluster: Cluster, hosts: util.Collection[Host]): Unit = {}
  override def distance(host: Host): HostDistance                         = Null[HostDistance]
  override def onAdd(host: Host): Unit                                    = {}
  override def onUp(host: Host): Unit                                     = {}
  override def onDown(host: Host): Unit                                   = {}
  override def onRemove(host: Host): Unit                                 = {}
  override def close(): Unit                                              = {}
}

class MyReconnectionPolicy extends ReconnectionPolicy {
  override def init(cluster: Cluster): Unit = {}
  override def newSchedule(): ReconnectionPolicy.ReconnectionSchedule =
    Null[ReconnectionPolicy.ReconnectionSchedule]
  override def close(): Unit = {}
}

class MyRetryPolicy extends RetryPolicy {
  override def init(cluster: Cluster): Unit = {}
  override def onReadTimeout(
      statement: Statement,
      cl: ConsistencyLevel,
      requiredResponses: Int,
      receivedResponses: Int,
      dataRetrieved: Boolean,
      nbRetry: Int): RetryPolicy.RetryDecision = Null[RetryPolicy.RetryDecision]
  override def onWriteTimeout(
      statement: Statement,
      cl: ConsistencyLevel,
      writeType: WriteType,
      requiredAcks: Int,
      receivedAcks: Int,
      nbRetry: Int): RetryPolicy.RetryDecision = Null[RetryPolicy.RetryDecision]
  override def onUnavailable(
      statement: Statement,
      cl: ConsistencyLevel,
      requiredReplica: Int,
      aliveReplica: Int,
      nbRetry: Int): RetryPolicy.RetryDecision = Null[RetryPolicy.RetryDecision]
  override def onRequestError(
      statement: Statement,
      cl: ConsistencyLevel,
      e: DriverException,
      nbRetry: Int): RetryPolicy.RetryDecision = Null[RetryPolicy.RetryDecision]
  override def close(): Unit                   = {}
}

class MySpeculativeExecutionPolicy extends SpeculativeExecutionPolicy {
  override def init(cluster: Cluster): Unit = {}
  override def newPlan(
      loggedKeyspace: String,
      statement: Statement): SpeculativeExecutionPolicy.SpeculativeExecutionPlan =
    Null[SpeculativeExecutionPolicy.SpeculativeExecutionPlan]
  override def close(): Unit = {}
}

class MySSLOptions extends SSLOptions {
  override def newSSLHandler(channel: SocketChannel): SslHandler = Null[SslHandler]
}

class MyThreadingOptions extends ThreadingOptions

class MyTimestampGenerator extends TimestampGenerator {
  override def next(): Long = 0L
}
