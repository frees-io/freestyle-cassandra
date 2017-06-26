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

import classy.DecodeError.WrongType
import classy.{Decoder, Read}
import classy.config._
import com.typesafe.config.Config

import scala.util.{Failure, Success, Try}

package object implicits {

  def instanceRead[T](implicit m: reflect.Manifest[T]): Read[Config, T] =
    read[String, T] { value =>
      Try(Class.forName(value).newInstance().asInstanceOf[T]) match {
        case Failure(_) =>
          Decoder.fail(WrongType(s"subclass of ${m.runtimeClass.getName}", Some(value)))
        case Success(c) => Decoder.const(c)
      }
    }

  implicit def stringListRead[T](implicit list: List[(String, T)]): Read[Config, T] =
    read[String, T] { value =>
      list.find(_._1 == value) match {
        case Some((_, v)) => Decoder.const(v)
        case None         => Decoder.fail(WrongType(list.map(_._1).mkString(" | "), Some(value)))
      }
    }

  def read[A, B](f: A => ConfigDecoder[B])(implicit R: Read[Config, A]): Read[Config, B] =
    Read.instance[Config, B](path => readConfig[A](path).flatMap(f))

}
