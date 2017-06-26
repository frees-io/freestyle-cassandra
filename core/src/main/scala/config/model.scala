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
package config

import java.net.{InetAddress, InetSocketAddress}

import com.datastax.driver.core.HostDistance

sealed trait ContactPoints                                         extends Product with Serializable
case class ContactPointList(list: List[InetAddress])               extends ContactPoints
case class ContactPointWithPortList(list: List[InetSocketAddress]) extends ContactPoints

case class Credentials(username: String, password: String)

case class ConnectionsPerHost(distance: HostDistance, core: Int, max: Int)
case class CoreConnectionsPerHost(distance: HostDistance, newCoreConnections: Int)
case class MaxConnectionsPerHost(distance: HostDistance, newMaxConnections: Int)
case class MaxRequestsPerConnection(distance: HostDistance, newMaxRequests: Int)
case class NewConnectionThreshold(distance: HostDistance, newValue: Int)
