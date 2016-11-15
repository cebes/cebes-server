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

import scala.collection.mutable
import scala.reflect.runtime._

/**
  * The generic parser for parsing Cebes' Expression into whatever
  */
trait ExpressionParser {

  /**
    * Parse an expression (depth-first)
    * This is the only function clients need to use
    */
  def parse(expr: Expression): Unit = {
    val stack = new mutable.Stack[Expression]()
    val visited = mutable.HashSet[Expression]()
    stack.push(expr)
    while(stack.nonEmpty) {
      val p = stack.head
      if (p.children.isEmpty || visited.contains(p)) {
        traverse(stack.pop())
        visited.remove(p)
      } else {
        stack.pushAll(p.children)
        visited.add(p)
      }
    }
  }

  protected def traverse(expr: Expression): Unit
}

/**
  * expression parser that uses Reflection to select the right
  * "visitX" function to call for each sub-type of Expression
  */
abstract class AbstractExpressionParser extends ExpressionParser {

  /**
    * map of visit methods, from parameter type -> method mirror
    */
  private lazy val visitMethods: Map[universe.Type, universe.MethodMirror] = {
    val thisMirror = currentMirror.reflect(this)

    currentMirror.classSymbol(getClass).toType.members.filter { m =>
      m.isMethod && m.name.decodedName.toString.startsWith("visit")
    }.map(_.asMethod).filter { m =>
      m.paramLists.nonEmpty &&
        m.paramLists.head.length == 1 &&
        m.paramLists.head.head.info <:< universe.typeOf[Expression]
    }.map { m =>
      m.paramLists.head.head.info -> thisMirror.reflectMethod(m)
    }.toMap
  }

  override protected def traverse(expr: Expression): Unit = {
    val exprType = currentMirror.reflect(expr).symbol.toType
    visitMethods.find(_._1 =:= exprType).map(_._2) match {
      case Some(method) => method(expr)
      case None =>
        throw new RuntimeException(s"Visit method not found for type ${exprType.toString}. " +
          s"Please add a new visit(expr: ${exprType.toString}) function into the parser class")
    }
  }
}
