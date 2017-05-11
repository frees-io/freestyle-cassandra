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

import java.nio.ByteBuffer

import com.datastax.driver.core._
import freestyle.cassandra.macros.Model._
import freestyle.cassandra.model._
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}

import scala.collection.JavaConverters._

class TableClassSpec extends WordSpec with Matchers with OneInstancePerTest with MockFactory {

  import freestyle.cassandra.codecs._

  implicit val sessionMock: Session = stub[Session]

  (sessionMock.prepare(_: String)).when(*).returns(new PreparedStatementTest)

  "TableClass annotation" should {

    "generate a TableClass1 with the right methods" in {

      val table: Table1 = Table1(id = 1)
      table shouldBe a[TableClass1[_]]
      val statement: AnyRef = table.boundedInsert
      statement shouldBe a[BoundStatementTest]
      val values: Map[String, ByteBuffer] =
        statement.asInstanceOf[BoundStatementTest].getValues.asScala.toMap
      values.get("id") shouldBe Some(longCodec.serialize(table.id))
    }

    "generate a TableClass2 with the right methods" in {

      val table: Table2 = Table2(1, "username")
      table shouldBe a[TableClass2[_, _]]
      val statement: AnyRef = table.boundedInsert
      statement shouldBe a[BoundStatementTest]
      val values: Map[String, ByteBuffer] =
        statement.asInstanceOf[BoundStatementTest].getValues.asScala.toMap
      values.get("id") shouldBe Some(longCodec.serialize(table.id))
      values.get("username") shouldBe Some(stringCodec.serialize(table.username))
    }

    "generate a TableClass3 with the right methods" in {

      val table: Table3 = Table3(1, "username", 20)
      table shouldBe a[TableClass3[_, _, _]]
      val statement: AnyRef = table.boundedInsert
      statement shouldBe a[BoundStatementTest]
      val values: Map[String, ByteBuffer] =
        statement.asInstanceOf[BoundStatementTest].getValues.asScala.toMap
      values.get("id") shouldBe Some(longCodec.serialize(table.id))
      values.get("username") shouldBe Some(stringCodec.serialize(table.username))
      values.get("age") shouldBe Some(intCodec.serialize(table.age))
    }

    "generate a TableClass3 with the right methods when there are 3 different types" in {

      val table: Table4 = Table4(1, "firstName", "lastName", 20)
      table shouldBe a[TableClass3[_, _, _]]
      val statement: AnyRef = table.boundedInsert
      statement shouldBe a[BoundStatementTest]
      val values: Map[String, ByteBuffer] =
        statement.asInstanceOf[BoundStatementTest].getValues.asScala.toMap
      values.get("id") shouldBe Some(longCodec.serialize(table.id))
      values.get("firstName") shouldBe Some(stringCodec.serialize(table.firstName))
      values.get("lastName") shouldBe Some(stringCodec.serialize(table.lastName))
      values.get("age") shouldBe Some(intCodec.serialize(table.age))
    }

  }

}
