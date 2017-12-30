/* Copyright 2016 The Cebes Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, version 2.0 (the "License").
 * You may not use this work except in compliance with the License,
 * which is available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 *
 * Created by phvu on 14/11/2016.
 */

package io.cebes.df.expressions

import java.lang.reflect.InvocationTargetException

import scala.collection.mutable
import scala.reflect.runtime._
import scala.util.{Failure, Success, Try}

/**
  * The generic parser for parsing Cebes' Expression into whatever
  *
  * @tparam T Type of the result
  */
trait StackExpressionParser[T] {

  /**
    * Parse an expression (depth-first)
    * This is the only function clients need to use
    */
  def parse(expr: Expression): T = {
    val stack = new mutable.Stack[Expression]()
    val visited = mutable.HashSet[Expression]()
    val results = new mutable.Stack[T]()

    stack.push(expr)
    while (stack.nonEmpty) {
      val p = stack.head
      if (p.children.isEmpty || visited.contains(p)) {
        traverse(stack.pop(), results)
        visited.remove(p)
      } else {
        stack.pushAll(p.children)
        visited.add(p)
      }
    }

    if (results.lengthCompare(1) != 0) {
      throw new IllegalArgumentException("There is an error when parsing the expression")
    }
    results.head
  }

  protected def traverse(expr: Expression, results: mutable.Stack[T]): Unit
}

/**
  * Expression parser that uses Reflection to select the right
  * "visitX(expr: X)" function to call for each sub-type of Expression
  *
  * The visit() methods of parsers that are subclasses of this class has to be in this form:
  *
  * {{{
  *   def visitExpressionX(expr: ExpressionX, parsedChildren: Seq[T]): Option[T] {
  *     ...
  *   }
  * }}}
  *
  * Note that we use the exact equality (=:=) to check the type of the
  * arguments when searching for the "visitX" function to call.
  *
  * This means if you only have visit(expr: Expression), and
  * you call `parse()` on X - a subclass of Expression - then a [[RuntimeException]] will be thrown.
  * It requires you to have a `visit(expr: X)` function.
  */
abstract class AbstractExpressionParser[T](implicit typeTag: universe.TypeTag[T]) extends StackExpressionParser[T] {

  /**
    * map of visit methods, from parameter type -> method mirror
    */
  private lazy val visitMethods: Map[universe.Type, universe.MethodMirror] = {
    val thisMirror = currentMirror.reflect(this)

    currentMirror.classSymbol(getClass).toType.members.filter { m =>
      m.isMethod && m.name.decodedName.toString.startsWith("visit")
    }.map(_.asMethod).filter { m =>
      val paramLists = m.paramLists
      paramLists.nonEmpty &&
        paramLists.head.nonEmpty &&
        paramLists.head.head.info <:< universe.typeOf[Expression] &&
        filterMethod(m)
    }.map { m =>
      m.paramLists.head.head.info -> thisMirror.reflectMethod(m)
    }.toMap
  }

  override protected def traverse(expr: Expression, results: mutable.Stack[T]): Unit = {
    val exprType = currentMirror.reflect(expr).symbol.toType
    visitMethods.find(_._1 =:= exprType).map(_._2) match {
      case Some(method) => invoke(expr, method, results)
      case None =>
        throw new RuntimeException(s"Visit method not found for type ${exprType.toString}. " +
          s"Please add a new visit() function into the parser class")
    }
  }

  private def filterMethod(method: universe.MethodSymbol): Boolean = {
    val paramList = method.paramLists.head
    paramList.lengthCompare(2) == 0 &&
      paramList.last.info =:= universe.typeOf[Seq[T]] &&
      method.returnType =:= universe.typeOf[Option[T]]
  }

  private def invoke(expr: Expression, method: universe.MethodMirror, results: mutable.Stack[T]): Unit = {
    if (results.lengthCompare(expr.children.size) < 0) {
      throw new IllegalStateException(s"Invalid expression at " +
        s"somewhere near ${expr.name}(${expr.children.map(_.name).mkString(", ")})")
    }

    val parsedChildren = expr.children.map(_ => results.pop())
    Try(method(expr, parsedChildren)) match {
      case Success(t) => t.asInstanceOf[Option[T]] match {
        case Some(result) => results.push(result)
        case None => // do nothing
      }
      case Failure(f: InvocationTargetException) => throw f.getCause
      case Failure(f) => throw f
    }
  }
}
