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
 * Created by phvu on 04/12/2016.
 */

package io.cebes.df.expressions

case class Coalesce(children: Seq[Expression]) extends Expression

case class InputFileName() extends LeafExpression

case class MonotonicallyIncreasingID() extends LeafExpression

case class NaNvl(left: Expression, right: Expression) extends BinaryExpression

case class Rand(seed: Long) extends LeafExpression

case class Randn(seed: Long) extends LeafExpression

case class SparkPartitionID() extends LeafExpression

case class RawExpression(expr: String) extends LeafExpression
