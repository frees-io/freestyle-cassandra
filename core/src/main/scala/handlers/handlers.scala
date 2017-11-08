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
package handlers

import java.nio.ByteBuffer

import cats.instances.list._
import cats.syntax.flatMap._
import cats.syntax.foldable._
import cats.syntax.traverse._
import cats.{~>, FlatMap, MonadError}
import com.datastax.driver.core._
import com.google.common.util.concurrent.ListenableFuture
import freestyle.cassandra.api._
import freestyle.cassandra.codecs.ByteBufferCodec
import freestyle.cassandra.query.mapper.{DatastaxRowReader, FromReader}
import freestyle.cassandra.query.model.SerializableValueBy

import scala.collection.JavaConverters._

class SessionAPIHandler[M[_]: FlatMap](
    implicit H: ListenableFuture[?] ~> M,
    ME: MonadError[M, Throwable])
    extends SessionAPI.Handler[SessionAPIOps[M, ?]] {

  def init: SessionAPIOps[M, Session] = kleisli(s => H(s.initAsync()))

  def close: SessionAPIOps[M, Unit] = closeFuture2unit[M, Session](_.closeAsync())

  def prepare(query: String): SessionAPIOps[M, PreparedStatement] =
    kleisli(s => H(s.prepareAsync(query)))

  def prepareStatement(statement: RegularStatement): SessionAPIOps[M, PreparedStatement] =
    kleisli(s => H(s.prepareAsync(statement)))

  def execute(query: String): SessionAPIOps[M, ResultSet] =
    kleisli(s => H(s.executeAsync(query)))

  def executeWithValues(query: String, values: Any*): SessionAPIOps[M, ResultSet] =
    kleisli(s => H(s.executeAsync(query, values)))

  def executeWithMap(query: String, values: Map[String, AnyRef]): SessionAPIOps[M, ResultSet] =
    kleisli(s => H(s.executeAsync(query, values.asJava)))

  def executeStatement(statement: Statement): SessionAPIOps[M, ResultSet] =
    kleisli(s => H(s.executeAsync(statement)))

  def executeWithByteBuffer(
      query: String,
      values: List[SerializableValueBy[Int]],
      consistencyLevel: Option[ConsistencyLevel] = None): SessionAPIOps[M, ResultSet] =
    kleisli { session =>
      values.traverse(_.serializableValue.serialize[M]).flatMap { values =>
        val st = ByteBufferSimpleStatement(query, values.toArray)
        consistencyLevel.foreach(st.setConsistencyLevel)
        H(session.executeAsync(st))
      }
    }

  case class ByteBufferSimpleStatement(query: String, values: Array[ByteBuffer])
      extends SimpleStatement(query, values) {
    override def getValues(
        protocolVersion: ProtocolVersion,
        codecRegistry: CodecRegistry): Array[ByteBuffer] = values
  }

}

class ClusterAPIHandler[M[_]](implicit H: ListenableFuture[?] ~> M, ME: MonadError[M, Throwable])
    extends ClusterAPI.Handler[ClusterAPIOps[M, ?]] {

  def connect: ClusterAPIOps[M, Session] = kleisli(c => H(c.connectAsync()))

  def connectKeyspace(keyspace: String): ClusterAPIOps[M, Session] =
    kleisli(c => H(c.connectAsync(keyspace)))

  def close: ClusterAPIOps[M, Unit] = closeFuture2unit[M, Cluster](_.closeAsync())

  def configuration: ClusterAPIOps[M, Configuration] =
    kleisli(c => ME.catchNonFatal(c.getConfiguration))

  def metadata: ClusterAPIOps[M, Metadata] =
    kleisli(c => ME.catchNonFatal(c.getMetadata))

  def metrics: ClusterAPIOps[M, Metrics] =
    kleisli(c => ME.catchNonFatal(c.getMetrics))

}

class StatementAPIHandler[M[_]](implicit ME: MonadError[M, Throwable])
    extends StatementAPI.Handler[M] {

  def bind(preparedStatement: PreparedStatement): M[BoundStatement] =
    ME.catchNonFatal(preparedStatement.bind())

  def setByteBufferByIndex(
      boundStatement: BoundStatement,
      index: Int,
      bytes: ByteBuffer): M[BoundStatement] =
    ME.catchNonFatal(boundStatement.setBytesUnsafe(index, bytes))

  def setByteBufferByName(
      boundStatement: BoundStatement,
      name: String,
      bytes: ByteBuffer): M[BoundStatement] =
    ME.catchNonFatal(boundStatement.setBytesUnsafe(name, bytes))

  def setValueByIndex[T](
      boundStatement: BoundStatement,
      index: Int,
      value: T,
      codec: ByteBufferCodec[T]): M[BoundStatement] =
    ME.flatMap(codec.serialize[M](value))(setByteBufferByIndex(boundStatement, index, _))

  def setValueByName[T](
      boundStatement: BoundStatement,
      name: String,
      value: T,
      codec: ByteBufferCodec[T]): M[BoundStatement] =
    ME.flatMap(codec.serialize[M](value))(setByteBufferByName(boundStatement, name, _))

  def setByteBufferListByIndex(
      statement: PreparedStatement,
      values: List[SerializableValueBy[Int]]): M[BoundStatement] =
    setByteBufferList(statement, values, setByteBufferByIndex)

  def setByteBufferListByName(
      statement: PreparedStatement,
      values: List[SerializableValueBy[String]]): M[BoundStatement] =
    setByteBufferList(statement, values, setByteBufferByName)

  private[this] def setByteBufferList[T](
      statement: PreparedStatement,
      values: List[SerializableValueBy[T]],
      setValue: (BoundStatement, T, ByteBuffer) => M[BoundStatement]): M[BoundStatement] =
    ME.flatMap(bind(statement)) { boundSt =>
      values.foldM(boundSt) { (b, v) =>
        ME.flatMap(v.serializableValue.serialize[M])(setValue(b, v.position, _))
      }
    }

}

class ResultSetAPIHandler[M[_]](implicit ME: MonadError[M, Throwable])
    extends ResultSetAPI.Handler[ResultSetAPIOps[M, ?]] {

  def read[A](FR: FromReader[A]): ResultSetAPIOps[M, A] =
    kleisli { resultSet =>
      ME.flatMap(ME.catchNonFatal(resultSet.one())) {
        Option(_)
          .map(readRow(_, FR))
          .getOrElse(ME.raiseError(new IllegalStateException("Row is empty")))
      }
    }

  def readOption[A](FR: FromReader[A]): ResultSetAPIOps[M, Option[A]] =
    kleisli { resultSet =>
      ME.flatMap(ME.catchNonFatal(resultSet.one())) {
        Option(_)
          .map(row => ME.map(readRow(row, FR))(Option(_)))
          .getOrElse(ME.pure(None))
      }
    }

  def readList[A](FR: FromReader[A]): ResultSetAPIOps[M, List[A]] = {
    import scala.collection.JavaConverters._
    kleisli { resultSet =>
      ME.flatMap(ME.catchNonFatal(resultSet.iterator().asScala.toList))(_.traverse(readRow(_, FR)))
    }
  }

  private[this] def readRow[A](row: Row, fromReader: FromReader[A]): M[A] =
    fromReader(DatastaxRowReader(row))
}
