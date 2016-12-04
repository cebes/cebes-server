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

case class ConcatWs(sep: String, children: Seq[Expression]) extends Expression

case class Contains(left: Expression, right: Expression) extends BinaryExpression

case class Decode(child: Expression, charset: String) extends UnaryExpression

case class Encode(child: Expression, charset: String) extends UnaryExpression

case class EndsWith(left: Expression, right: Expression) extends BinaryExpression

case class FormatNumber(child: Expression, precision: Int) extends UnaryExpression

case class FormatString(format: String, children: Seq[Expression]) extends Expression

case class InitCap(child: Expression) extends UnaryExpression

case class StringInstr(child: Expression, subStr: String) extends UnaryExpression

case class Length(child: Expression) extends UnaryExpression

case class Like(child: Expression, literal: String) extends UnaryExpression

case class Lower(child: Expression) extends UnaryExpression

case class Levenshtein(left: Expression, right: Expression) extends BinaryExpression

case class StringLocate(substr: String, child: Expression, pos: Int) extends UnaryExpression

case class StringLPad(child: Expression, len: Int, pad: String) extends UnaryExpression

case class StringTrimLeft(child: Expression) extends UnaryExpression

case class RegExpExtract(child: Expression, expr: String, groupIdx: Int) extends UnaryExpression

case class RegExpReplace(child: Expression, pattern: String, replacement: String) extends UnaryExpression

case class RLike(child: Expression, literal: String) extends UnaryExpression

case class StartsWith(left: Expression, right: Expression) extends BinaryExpression

case class StringRPad(child: Expression, len: Int, pad: String) extends UnaryExpression

case class StringRepeat(child: Expression, n: Int) extends UnaryExpression

case class StringReverse(child: Expression) extends UnaryExpression

case class StringTrimRight(child: Expression) extends UnaryExpression

case class SoundEx(child: Expression) extends UnaryExpression

case class StringSplit(child: Expression, pattern: String) extends UnaryExpression

case class StringTranslate(child: Expression, matching: String, replace: String) extends UnaryExpression

case class StringTrim(child: Expression) extends UnaryExpression

case class Substring(child: Expression, pos: Int, len: Int) extends UnaryExpression

case class SubstringIndex(child: Expression, delim: String, count: Int) extends UnaryExpression

case class UnBase64(child: Expression) extends UnaryExpression

case class Upper(child: Expression) extends UnaryExpression
