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

package freestyle.cassandra.macros

object Model {

  @TableClass
  case class Table1(id: Long)

  @TableClass
  case class Table2(id: Long, username: String)

  @TableClass
  case class Table3(id: Long, username: String, age: Int)

  @TableClass
  case class Table4(id: Long, firstName: String, lastName: String, age: Int)

}