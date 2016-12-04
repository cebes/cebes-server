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

case class Ascii(child: Expression) extends UnaryExpression

case class Base64(child: Expression) extends UnaryExpression

case class Concat(children: Seq[Expression]) extends Expression

case class ConcatWs(children: Seq[Expression]) extends Expression

case class Contains(left: Expression, right: Expression) extends BinaryExpression

case class Decode(left: Expression, right: Expression) extends BinaryExpression

case class Encode(left: Expression, right: Expression) extends BinaryExpression

case class EndsWith(left: Expression, right: Expression) extends BinaryExpression

case class FormatNumber(left: Expression, right: Expression) extends BinaryExpression

case class FormatString(children: Seq[Expression]) extends Expression

case class InitCap(child: Expression) extends UnaryExpression

case class StringInstr(left: Expression, right: Expression) extends BinaryExpression

case class Length(child: Expression) extends UnaryExpression

case class Like(child: Expression, literal: String) extends UnaryExpression

case class Lower(child: Expression) extends UnaryExpression

case class Levenshtein(left: Expression, right: Expression) extends BinaryExpression

case class StringLocate(substr: Expression, str: Expression, pos: Expression) extends Expression {
  override def children = Seq(substr, str, pos)
}

case class StringLPad(str: Expression, len: Expression, pad: Expression) extends Expression {
  override def children = Seq(str, len, pad)
}

case class StringTrimLeft(child: Expression) extends UnaryExpression

case class RegExpExtract(col: Expression, expr: Expression, groupIdx: Expression) extends Expression {
  override def children = Seq(col, expr, groupIdx)
}

case class RegExpReplace(col: Expression, pattern: Expression, replacement: Expression) extends Expression {
  override def children = Seq(col, pattern, replacement)
}

case class RLike(child: Expression, literal: String) extends UnaryExpression

case class StartsWith(left: Expression, right: Expression) extends BinaryExpression

case class StringRPad(str: Expression, len: Expression, pad: Expression) extends Expression {
  override def children = Seq(str, len, pad)
}

case class StringRepeat(left: Expression, right: Expression) extends BinaryExpression

case class StringReverse(child: Expression) extends UnaryExpression

case class StringTrimRight(child: Expression) extends UnaryExpression

case class SoundEx(child: Expression) extends UnaryExpression

case class StringSplit(left: Expression, right: Expression) extends BinaryExpression

case class StringTranslate(src: Expression, matching: Expression, replace: Expression) extends Expression {
  override def children = Seq(src, matching, replace)
}

case class StringTrim(child: Expression) extends UnaryExpression

case class Substring(str: Expression, pos: Expression, len: Expression) extends Expression {
  override def children: Seq[Expression] = Seq(str, pos, len)
}

case class SubstringIndex(str: Expression, delim: Expression, count: Expression) extends Expression {
  override def children = Seq(str, delim, count)
}

case class UnBase64(child: Expression) extends UnaryExpression

case class Upper(child: Expression) extends UnaryExpression
