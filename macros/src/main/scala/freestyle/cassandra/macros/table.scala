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

package freestyle
package cassandra
package macros

import scala.annotation.StaticAnnotation
import scala.language.experimental.macros
import scala.language.postfixOps
import scala.reflect.macros.whitebox

class table extends StaticAnnotation {

  def macroTransform(annottees: Any*): Any = macro table.impl

}

object table {

  def impl(c: whitebox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {

    import c.universe._

    def extractCaseClassesParts(classDecl: ClassDef) = classDecl match {
      case q"case class $className(..$fields) extends ..$parents { ..$body }" =>
        (className, fields, parents, body)
    }

    def createMethod(
        className: c.universe.TypeName,
        paramTypes: List[c.universe.Tree],
        params: List[ValDef]) = {

      val codecs = paramTypes.zipWithIndex.map {
        case (t, p) => q"""${TermName(s"codec$p")}: ByteBufferCodec[$t]"""
      }

      val paramNames     = params.map(_.name.toString)
      val paramWildcards = params.map(_ => "?")

      val setValues = params.flatMap { p =>
        p.children.headOption.map { t =>
          val fieldName = p.name.toString
          q"""setValue[$t](st, $fieldName, ${p.name});"""
        }
      }

      val sql =
        s"INSERT INTO ${className.toString} (${paramNames.mkString(",")}) VALUES (${paramWildcards.mkString(",")})"

      q"""
         override def boundedInsert(implicit S: Session, ..$codecs): BoundStatement = {
           val st = S.prepare($sql).bind()
           ..$setValues
           st
         }
      """
    }

    def modifiedDeclaration(classDecl: ClassDef) = {

      val (className, fields, parents, body) = extractCaseClassesParts(classDecl)

      val params = fields.asInstanceOf[List[ValDef]] map { p =>
        p.duplicate
      }

      val paramTypes =
        params flatMap (_.children.headOption) groupBy (_.toString()) flatMap (_._2.headOption) toList

      val tableClass = paramTypes.size match {
        case n if n > 0 && n <= 3 => TypeName(s"TableClass$n")
        case n =>
          c.abort(c.enclosingPosition, s"Classes with $n different field types are not supported")
      }

      val newMethod = createMethod(className, paramTypes, params)

      val classDef = q"""
        import freestyle.cassandra.codecs.ByteBufferCodec
        import freestyle.cassandra.model._
        import com.datastax.driver.core._
        case class $className ( ..$params )
          extends $tableClass[..$paramTypes]
          with ..$parents {
          ..${body :+ newMethod}
        }
      """

      c.Expr[Any](classDef)
    }

    annottees map (_.tree) toList match {
      case (classDecl: ClassDef) :: Nil => modifiedDeclaration(classDecl)
      case _                            => c.abort(c.enclosingPosition, "Invalid annottee")
    }

  }

}
