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
 * Created by phvu on 17/11/2016.
 */

package io.cebes.df.expressions

case class CaseWhen(branches: Seq[(Expression, Expression)],
                    elseValue: Option[Expression] = None) extends Expression {

  override def children: Seq[Expression] = branches.flatMap(b => b._1 :: b._2 :: Nil) ++ elseValue
}

case class GetItem(left: Expression, right: Expression) extends BinaryExpression

case class GetField(child: Expression, fieldName: String) extends UnaryExpression
