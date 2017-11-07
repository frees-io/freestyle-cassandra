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
package api

import freestyle._
import freestyle.cassandra.query.mapper.FromReader

trait ResultSetAPI[FF$1941[_]] extends _root_.freestyle.internal.EffectLike[FF$1941] {
  def read[A](implicit FR: FromReader[A]): FS[A]

  def readOption[A](implicit FR: FromReader[A]): FS[Option[A]]

  def readList[A](implicit FR: FromReader[A]): FS[List[A]]
}
@ _root_.java.lang.SuppressWarnings(
  _root_.scala.Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Throw"))
object ResultSetAPI {

  sealed trait Op[_] extends _root_.scala.Product with _root_.java.io.Serializable {
    val FSAlgebraIndex1940: _root_.scala.Int
  }

  final case class ReadOp[A](FR: FromReader[A]) extends _root_.scala.AnyRef with Op[A] {
    override val FSAlgebraIndex1940: _root_.scala.Int = 0
  }

  final case class ReadOptionOp[A](FR: FromReader[A])
      extends _root_.scala.AnyRef
      with Op[Option[A]] {
    override val FSAlgebraIndex1940: _root_.scala.Int = 1
  }

  final case class ReadListOp[A](FR: FromReader[A]) extends _root_.scala.AnyRef with Op[List[A]] {
    override val FSAlgebraIndex1940: _root_.scala.Int = 2
  }

  type OpTypes = _root_.iota.TConsK[Op, _root_.iota.TNilK]

  trait Handler[MM$1947[_]] extends _root_.freestyle.FSHandler[Op, MM$1947] {
    protected[this] def read[A](FR: FromReader[A]): MM$1947[A]

    protected[this] def readOption[A](FR: FromReader[A]): MM$1947[Option[A]]

    protected[this] def readList[A](FR: FromReader[A]): MM$1947[List[A]]

    override def apply[AA$1948](fa$1949: Op[AA$1948]): MM$1947[AA$1948] =
      ((fa$1949.FSAlgebraIndex1940: @ _root_.scala.annotation.switch) match {
        case 0 =>
          val fresh1950: ReadOp[_root_.scala.Any] = fa$1949.asInstanceOf[ReadOp[_root_.scala.Any]]
          read(fresh1950.FR)
        case 1 =>
          val fresh1951: ReadOptionOp[_root_.scala.Any] =
            fa$1949.asInstanceOf[ReadOptionOp[_root_.scala.Any]]
          readOption(fresh1951.FR)
        case 2 =>
          val fresh1952: ReadListOp[_root_.scala.Any] =
            fa$1949.asInstanceOf[ReadListOp[_root_.scala.Any]]
          readList(fresh1952.FR)
        case i =>
          throw new _root_.java.lang.Exception(
            "freestyle internal error: index " + i.toString() + " out of bounds for " + this
              .toString())
      }).asInstanceOf[MM$1947[AA$1948]]
  }

  class To[LL$1943[_]](implicit ii$1944: _root_.freestyle.InjK[Op, LL$1943])
      extends ResultSetAPI[LL$1943] {
    private[this] val toInj1945 = _root_.freestyle.FreeS.inject[Op, LL$1943](ii$1944)

    override def read[A](implicit FR: FromReader[A]): FS[A] = toInj1945(ReadOp[A](FR))

    override def readOption[A](implicit FR: FromReader[A]): FS[Option[A]] =
      toInj1945(ReadOptionOp[A](FR))

    override def readList[A](implicit FR: FromReader[A]): FS[List[A]] = toInj1945(ReadListOp[A](FR))
  }

  implicit def to[LL$1943[_]](implicit ii$1944: _root_.freestyle.InjK[Op, LL$1943]): To[LL$1943] =
    new To[LL$1943]

  def apply[LL$1943[_]](implicit ev$1946: ResultSetAPI[LL$1943]): ResultSetAPI[LL$1943] = ev$1946
}
