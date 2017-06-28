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
import com.datastax.driver.core.Statement
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

  implicit def stringListRead[T](implicit map: Map[String, T]): Read[Config, T] =
    read[String, T] { value =>
      map.get(value) match {
        case Some(v) => Decoder.const(v)
        case None    => Decoder.fail(WrongType(map.keys.mkString(" | "), Some(value)))
      }
    }

  def read[A, B](f: A => ConfigDecoder[B])(implicit R: Read[Config, A]): Read[Config, B] =
    Read.instance[Config, B](path => readConfig[A](path).flatMap(f))

  final class ConfigStatementOps(cs: ConfigStatement) {

    def applyConf(st: Statement): Statement = {

      import scala.collection.JavaConverters._

      cs.tracingEnabled foreach {
        case true  => st.enableTracing()
        case false => st.disableTracing()
      }
      cs.consistencyLevel foreach st.setConsistencyLevel
      cs.serialConsistencyLevel foreach st.setSerialConsistencyLevel
      cs.defaultTimestamp foreach st.setDefaultTimestamp
      cs.fetchSize foreach st.setFetchSize
      cs.idempotent foreach st.setIdempotent
      cs.outgoingPayload foreach (m => st.setOutgoingPayload(m.asJava))
      cs.pagingState foreach {
        case CodecPagingState(ps, Some(cr)) => st.setPagingState(ps, cr)
        case CodecPagingState(ps, None)     => st.setPagingState(ps)
        case RawPagingState(array)          => st.setPagingStateUnsafe(array)
      }
      cs.readTimeoutMillis foreach st.setReadTimeoutMillis
      cs.retryPolicy foreach st.setRetryPolicy
      st
    }

  }

  implicit def configStatementOps(cs: ConfigStatement): ConfigStatementOps =
    new ConfigStatementOps(cs)

}
