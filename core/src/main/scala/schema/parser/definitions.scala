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
package schema.parser

import scala.util.matching.Regex

object definitions {

  val unquotedName: String  = "([a-zA-Z_0-9]{1,48})"
  val quotedName: String    = s""""$unquotedName""""
  val name: String          = s""""?$unquotedName"?"""
  val replication: String   = """replication\s*=\s*(\{[A-Za-z0-9':, _]+\})"""
  val durableWrites: String = """durable_writes\s*=\s*(false|true)"""
  val withRep: String       = s"""WITH\\s+$replication"""
  val andRep: String        = s"""AND\\s+$replication"""
  val withDW: String        = s"""WITH\\s+$durableWrites"""
  val andDW: String         = s"""(AND\\s+$durableWrites)?"""

  val UnquotedNameRegex: Regex = unquotedName.r
  val QuotedNameRegex: Regex   = quotedName.r
  val NameRegex: Regex         = name.r
  val WithRepRegex: Regex      = withRep.r
  val AndRepRegex: Regex       = andRep.r
  val WithDWRegex: Regex       = withDW.r
  val AndDWRegex: Regex        = andDW.r

}
