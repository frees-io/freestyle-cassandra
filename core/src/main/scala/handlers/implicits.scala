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
import cats.syntax.foldable._
import cats.data.Kleisli
import cats.instances.list._
import cats.syntax.traverse._
import cats.{~>, MonadError}

import collection.JavaConverters._
import com.datastax.driver.core._
import com.google.common.util.concurrent.{AsyncFunction, Futures, ListenableFuture, MoreExecutors}
import freestyle.async.AsyncContext
import freestyle.cassandra.api.{ClusterAPI, ClusterAPIOps, SessionAPI, SessionAPIOps, StatementAPI}
import freestyle.cassandra.codecs.ByteBufferCodec
import freestyle.cassandra.query.model.SerializableValueBy

object implicits {

  import freestyle.cassandra.implicits._

  implicit def sessionAPIHandler[M[_]](
      implicit AC: AsyncContext[M],
      E: MonadError[M, Throwable]): SessionAPIHandler[M] =
    new SessionAPIHandler[M]

  implicit def clusterAPIHandler[M[_]](
      implicit AC: AsyncContext[M],
      E: MonadError[M, Throwable]): ClusterAPIHandler[M] =
    new ClusterAPIHandler[M]

  implicit def statementAPIHandler[M[_]](
      implicit E: MonadError[M, Throwable]): StatementAPIHandler[M] =
    new StatementAPIHandler[M]

  class SessionAPIHandler[M[_]](implicit H: ListenableFuture[?] ~> M, E: MonadError[M, Throwable])
      extends SessionAPI.Handler[SessionAPIOps[M, ?]] {

    def init: SessionAPIOps[M, Session] = Kleisli(s => H(s.initAsync()))

    def close: SessionAPIOps[M, Unit] = closeFuture2unit[M, Session](_.closeAsync())

    def prepare(query: String): SessionAPIOps[M, PreparedStatement] =
      Kleisli(s => H(s.prepareAsync(query)))

    def prepareStatement(statement: RegularStatement): SessionAPIOps[M, PreparedStatement] =
      Kleisli(s => H(s.prepareAsync(statement)))

    def execute(query: String): SessionAPIOps[M, ResultSet] =
      Kleisli(s => H(s.executeAsync(query)))

    def executeWithValues(query: String, values: Any*): SessionAPIOps[M, ResultSet] =
      Kleisli(s => H(s.executeAsync(query, values)))

    def executeWithMap(query: String, values: Map[String, AnyRef]): SessionAPIOps[M, ResultSet] =
      Kleisli(s => H(s.executeAsync(query, values.asJava)))

    def executeStatement(statement: Statement): SessionAPIOps[M, ResultSet] =
      Kleisli(s => H(s.executeAsync(statement)))

    def executeWithByteBuffer(
        query: String,
        values: List[SerializableValueBy[Int]]): SessionAPIOps[M, ResultSet] =
      Kleisli { session =>
        E.flatMap {
          values.traverse(_.serializableValue.serialize[M])
        } { values =>
          H(session.executeAsync(ByteBufferSimpleStatement(query, values.toArray)))
        }
      }

  }

  class ClusterAPIHandler[M[_]](implicit H: ListenableFuture[?] ~> M, E: MonadError[M, Throwable])
      extends ClusterAPI.Handler[ClusterAPIOps[M, ?]] {

    def connect: ClusterAPIOps[M, Session] = Kleisli(c => H(c.connectAsync()))

    def connectKeyspace(keyspace: String): ClusterAPIOps[M, Session] =
      Kleisli(c => H(c.connectAsync(keyspace)))

    def close: ClusterAPIOps[M, Unit] = closeFuture2unit[M, Cluster](_.closeAsync())

    def configuration: ClusterAPIOps[M, Configuration] =
      Kleisli(c => E.catchNonFatal(c.getConfiguration))

    def metadata: ClusterAPIOps[M, Metadata] =
      Kleisli(c => E.catchNonFatal(c.getMetadata))

    def metrics: ClusterAPIOps[M, Metrics] =
      Kleisli(c => E.catchNonFatal(c.getMetrics))

  }

  class StatementAPIHandler[M[_]](implicit E: MonadError[M, Throwable])
      extends StatementAPI.Handler[M] {

    def bind(preparedStatement: PreparedStatement): M[BoundStatement] =
      E.catchNonFatal(preparedStatement.bind())

    def setByteBufferByIndex(
        boundStatement: BoundStatement,
        index: Int,
        bytes: ByteBuffer): M[BoundStatement] =
      E.catchNonFatal(boundStatement.setBytesUnsafe(index, bytes))

    def setByteBufferByName(
        boundStatement: BoundStatement,
        name: String,
        bytes: ByteBuffer): M[BoundStatement] =
      E.catchNonFatal(boundStatement.setBytesUnsafe(name, bytes))

    def setValueByIndex[T](
        boundStatement: BoundStatement,
        index: Int,
        value: T,
        codec: ByteBufferCodec[T]): M[BoundStatement] =
      E.flatMap(codec.serialize[M](value))(setByteBufferByIndex(boundStatement, index, _))

    def setValueByName[T](
        boundStatement: BoundStatement,
        name: String,
        value: T,
        codec: ByteBufferCodec[T]): M[BoundStatement] =
      E.flatMap(codec.serialize[M](value))(setByteBufferByName(boundStatement, name, _))

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
      E.flatMap(bind(statement)) { boundSt =>
        values.foldM(boundSt) { (b, v) =>
          E.flatMap(v.serializableValue.serialize[M])(setValue(b, v.position, _))
        }
      }

  }

  private[this] case class ByteBufferSimpleStatement(query: String, values: Array[ByteBuffer])
      extends SimpleStatement(query, values) {
    override def getValues(
        protocolVersion: ProtocolVersion,
        codecRegistry: CodecRegistry): Array[ByteBuffer] = values
  }

  private[this] def closeFuture2unit[M[_], A](f: A => CloseFuture)(
      implicit H: ListenableFuture[?] ~> M): Kleisli[M, A, Unit] = {

    def listenableFuture(a: A): ListenableFuture[Unit] = Futures.transformAsync(
      f(a),
      new AsyncFunction[Void, Unit] {
        override def apply(input: Void): ListenableFuture[Unit] =
          Futures.immediateFuture((): Unit)
      },
      MoreExecutors.directExecutor()
    )

    Kleisli(c => H(listenableFuture(c)))

  }

}
