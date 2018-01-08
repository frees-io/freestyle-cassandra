/*
 * Copyright 2017-2018 47 Degrees, LLC. <http://www.47deg.com>
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

  sealed trait ValidatorType
  case object SchemaFile extends ValidatorType
  case object Metadata extends ValidatorType

  import MetaMacroInterpolator._

  class SchemaFileInterpolator(schemaPath: String) extends scala.annotation.StaticAnnotation {

    inline def apply(defn: Any): Any =
      meta(generateMacro(this, defn, getClass.getSimpleName, SchemaFile))
  }

  class SchemaMetadataInterpolator(configPath: String) extends scala.annotation.StaticAnnotation {

    inline def apply(defn: Any): Any =
      meta(generateMacro(this, defn, getClass.getSimpleName, Metadata))
  }

  object MetaMacroInterpolator {

    def generateMacro(stat: Stat, defn: Any, annotation: String, validatorType: ValidatorType): Term.Block = {

      val arg = stat match {
        case q"new $_(${Lit(argument: String)})" if argument.nonEmpty =>
          argument
        case _ =>
          abort(s"@$annotation annotation requires a valid path")
      }

      defn match {
        case t: Defn.Trait =>
          Term.Block(Seq(t, companion(t.name, arg, validatorType)))
        case _ =>
          abort(s"@$annotation must annotate a trait.")
      }
    }

    private[this] def inputStreamBlock(typeName: Type.Name, path: String): Term.Block =
      q"""
       import _root_.scala.util._
       val myPath = ${Lit.String(path)}
       Try(Option(${Term.Name(typeName.value)}.getClass.getResourceAsStream(myPath))).flatMap {
         case Some(is) => Success(is)
         case None => Failure(new IllegalArgumentException("Resource path " + myPath + " not found"))
       }
     """

    private[this] def validatorBlock(typeName: Type.Name, path: String, validator: ValidatorType): Term.Block = {

      def schemaFileValidator =
        q"""
          import _root_.java.io.InputStream
          val tryMonadError: MonadError[Try, Throwable] = _root_.cats.instances.try_.catsStdInstancesForTry
          val isF: Try[InputStream] = ${inputStreamBlock(typeName, path)}
          val schemaProvider: SchemaDefinitionProvider[Try] = TroySchemaProvider[Try](isF)(tryMonadError)
          TroySchemaValidator.instance(tryMonadError, schemaProvider)
       """

      def metadataValidator =
        q"""
          import _root_.java.io.InputStream
          val tryMonadError: MonadError[Try, Throwable] = _root_.cats.instances.try_.catsStdInstancesForTry
          val isF: Try[InputStream] = ${inputStreamBlock(typeName, path)}
          val schemaProvider: SchemaDefinitionProvider[Try] = MetadataSchemaProvider.metadataSchemaProvider[Try](isF)(tryMonadError)
          TroySchemaValidator.instance(tryMonadError, schemaProvider)
       """

      validator match {
        case SchemaFile => schemaFileValidator
        case Metadata => metadataValidator
      }
    }

    private[this] def companion(typeName: Type.Name, path: String, validator: ValidatorType) =
      q"""object ${Term.Name(typeName.value)} {

          import _root_.cats.MonadError
          import _root_.contextual.{Case, Prefix}
          import _root_.freestyle.cassandra.codecs.ByteBufferCodec
          import _root_.freestyle.cassandra.query.interpolator.{CQLInterpolator, CQLLiteral}
          import _root_.freestyle.cassandra.query.model.SerializableValue
          import _root_.freestyle.cassandra.schema.provider._
          import _root_.freestyle.cassandra.schema.validator._
          import _root_.java.nio.ByteBuffer
          import _root_.scala.util.Try

          val schemaValidator: SchemaValidator[Try] = ${validatorBlock(typeName, path, validator)}

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

  }

}
