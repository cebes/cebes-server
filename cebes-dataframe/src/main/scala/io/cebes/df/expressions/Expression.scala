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
 * Created by phvu on 13/11/2016.
 */

package io.cebes.df.expressions

// TODO: a few design choices to make:
// - Factor out the Expression into a separated TreeNode[] class, to handle tree-related matters
// - Handle semantic stuff: expected input types, output data type, etc...

/**
  *
  */
trait Expression {

  def children: Seq[Expression]

  /**
    * Name of this expression, default to be the class name
    */
  def name: String = getClass.getSimpleName

  // deserialize from JSON (clients)

  // get the state
}

abstract class LeafExpression extends Expression {

  def children: Seq[Expression] = Nil
}

abstract class UnaryExpression extends Expression {

  def child: Expression

  def children: Seq[Expression] = Seq(child)
}

abstract class BinaryExpression extends Expression {

  def left: Expression

  def right: Expression

  def children: Seq[Expression] = Seq(left, right)
}