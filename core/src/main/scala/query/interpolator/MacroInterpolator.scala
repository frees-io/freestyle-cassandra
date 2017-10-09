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
package query.interpolator

import scala.collection.immutable.Seq
import scala.meta._

object MacroInterpolator {

  def imports =
    q"""
        import cats.MonadError
        import contextual.{Case, Prefix}
        import freestyle.cassandra.codecs.ByteBufferCodec
        import freestyle.cassandra.query.interpolator.{CQLInterpolator, CQLLiteral}
        import freestyle.cassandra.query.model.SerializableValue
        import freestyle.cassandra.schema.provider.{SchemaDefinitionProvider, TroySchemaProvider}
        import freestyle.cassandra.schema.validator.{SchemaValidator, TroySchemaValidator}
        import java.nio.ByteBuffer
        import scala.util.Try
     """

  def schemaFileValidator(typeName: Type.Name, schemaPath: String) =
    q"""
        val tryMonadError: MonadError[Try, Throwable] = cats.instances.try_.catsStdInstancesForTry
        val schemaProvider: SchemaDefinitionProvider[Try] =
          TroySchemaProvider[Try](${Term.Name(typeName.value)}.getClass.getResourceAsStream(${Lit.String(schemaPath)}))(tryMonadError)
        val schemaValidator: SchemaValidator[Try] = TroySchemaValidator.instance(tryMonadError, schemaProvider)
     """

  def companion(typeName: Type.Name) =
    q"""
        object ${Term.Name(typeName.value)} {

          object cqlInterpolator extends CQLInterpolator(schemaValidator)

          implicit def embedArgsNamesInCql[T](implicit C: ByteBufferCodec[T]) = cqlInterpolator.embed[T](
            Case(CQLLiteral, CQLLiteral) { v =>
              new SerializableValue {
                override def serialize[M[_]](implicit E: MonadError[M, Throwable]): M[ByteBuffer] =
                  C.serialize(v)
              }
            }
          )

          final class CQLStringContext(sc: StringContext) {
            val cql = Prefix(cqlInterpolator, sc)
          }

          implicit def cqlStringContext(sc: StringContext): CQLStringContext =
            new CQLStringContext(sc)
        }
     """

  class SchemaFileInterpolator(schemaPath: String) extends scala.annotation.StaticAnnotation {

    inline def apply(defn: Any): Any = meta {
      val arg = this match {
        case q"new $_(${Lit(argument: String)})" if argument.nonEmpty =>
          argument
        case _ =>
          abort("You must provide a valid schema path")
      }

      defn match {
        case t: Defn.Trait =>
          Term.Block(Seq(t, imports, schemaFileValidator(t.name, arg), companion(t.name)))
        case _ =>
          println(defn.structure)
          abort("@SchemaInterpolator must annotate a trait.")
      }
    }
  }

}
