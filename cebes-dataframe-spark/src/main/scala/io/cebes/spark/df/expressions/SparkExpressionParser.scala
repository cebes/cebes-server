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
 * Created by phvu on 15/11/2016.
 */

package io.cebes.spark.df.expressions

import io.cebes.df.Column
import io.cebes.df.expressions._
import io.cebes.spark.df.schema.SparkSchemaUtils
import org.apache.spark.sql.{Column => SparkColumn, functions => sparkFunctions}


object SparkExpressionParser {

  /**
    * Transform a cebes Column into a Spark column
    */
  def toSparkColumn(column: Column): SparkColumn = {
    val parser = new SparkExpressionParser()
    parser.parse(column.expr)
    parser.getResult
  }

  def toSparkColumns(columns: Column*): Seq[SparkColumn] = columns.map(toSparkColumn)
}


class SparkExpressionParser extends StackExpressionParser[SparkColumn] {

  /////////////////////////////////////////////////////////////////////////////
  // visit functions
  /////////////////////////////////////////////////////////////////////////////

  protected def visitSparkPrimitiveExpression(expr: SparkPrimitiveExpression,
                                              parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(expr.sparkCol)
  }

  protected def visitLiteral(expr: Literal, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.lit(expr.value))
  }

  protected def visitUnresolvedColumnName(expr: UnresolvedColumnName,
                                          parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.col(expr.colName))
  }

  protected def visitSortOrder(expr: SortOrder, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(expr.direction match {
      case Ascending => parsedChildren.head.asc
      case Descending => parsedChildren.head.desc
    })
  }

  protected def visitUnaryMinus(expr: UnaryMinus, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(-parsedChildren.head)
  }

  protected def visitNot(expr: Not, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(!parsedChildren.head)
  }

  protected def visitEqualTo(expr: EqualTo, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head === parsedChildren.last)
  }

  protected def visitGreaterThan(expr: GreaterThan, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head > parsedChildren.last)
  }

  protected def visitLessThan(expr: LessThan, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head < parsedChildren.last)
  }

  protected def visitGreaterThanOrEqual(expr: GreaterThanOrEqual,
                                        parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head >= parsedChildren.last)
  }

  protected def visitLessThanOrEqual(expr: LessThanOrEqual,
                                     parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head <= parsedChildren.last)
  }

  protected def visitLessEqualNullSafe(expr: EqualNullSafe,
                                       parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head <=> parsedChildren.last)
  }

  protected def visitCaseWhen(expr: CaseWhen, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    require(parsedChildren.length >= 2, "Expected a list of branches")

    val branch = sparkFunctions.when(parsedChildren.head, parsedChildren(1))
    Some(parsedChildren.grouped(2).toSeq.tail.foldLeft(branch) { (b, conditionClause) =>
      conditionClause match {
        case Seq(condition, value) => b.when(condition, value)
        case Seq(otherwiseVal) => b.otherwise(otherwiseVal)
      }
    })
  }

  protected def visitIsNaN(expr: IsNaN, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.isNaN)
  }

  protected def visitIsNull(expr: IsNull, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.isNull)
  }

  protected def visitIsNotNull(expr: IsNotNull, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.isNotNull)
  }

  protected def visitOr(expr: Or, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head || parsedChildren.last)
  }

  protected def visitAnd(expr: And, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head && parsedChildren.last)
  }

  protected def visitAdd(expr: Add, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head + parsedChildren.last)
  }

  protected def visitSubtract(expr: Subtract, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head - parsedChildren.last)
  }

  protected def visitMultiply(expr: Multiply, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head * parsedChildren.last)
  }

  protected def visitDivide(expr: Divide, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head / parsedChildren.last)
  }

  protected def visitRemainder(expr: Remainder, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head % parsedChildren.last)
  }

  protected def visitIn(expr: In, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.isin(parsedChildren.tail: _*))
  }

  protected def visitLike(expr: Like, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.like(expr.literal))
  }

  protected def visitRLike(expr: RLike, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.rlike(expr.literal))
  }

  protected def visitGetItem(expr: GetItem, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.getItem(parsedChildren.last))
  }

  protected def visitGetField(expr: GetField, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.getField(expr.fieldName))
  }

  protected def visitContains(expr: Contains, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.contains(parsedChildren.last))
  }

  protected def visitStartsWith(expr: StartsWith, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.startsWith(parsedChildren.last))
  }

  protected def visitEndsWith(expr: EndsWith, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.endsWith(parsedChildren.last))
  }

  protected def visitMultiAlias(expr: MultiAlias, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.as(expr.aliases))
  }

  protected def visitAlias(expr: Alias, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.alias(expr.alias))
  }

  protected def visitCast(expr: Cast, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.cast(SparkSchemaUtils.cebesTypesToSpark(expr.to)))
  }

  protected def visitBitwiseOr(expr: BitwiseOr, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.bitwiseOR(parsedChildren.last))
  }

  protected def visitBitwiseAnd(expr: BitwiseAnd, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.bitwiseAND(parsedChildren.last))
  }

  protected def visitBitwiseXor(expr: BitwiseXor, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(parsedChildren.head.bitwiseXOR(parsedChildren.last))
  }

  protected def visitBitwiseNot(expr: BitwiseNot, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.bitwiseNOT(parsedChildren.head))
  }

  protected def visitApproxCountDistinct(expr: ApproxCountDistinct,
                                         parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.approxCountDistinct(parsedChildren.head, expr.relativeSD))
  }

  protected def visitAverage(expr: Average,
                             parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.avg(parsedChildren.head))
  }

  protected def visitCollectList(expr: CollectList,
                                 parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.collect_list(parsedChildren.head))
  }

  protected def visitCollectSet(expr: CollectSet,
                                parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.collect_set(parsedChildren.head))
  }

  protected def visitCorr(expr: Corr,
                          parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.corr(parsedChildren.head, parsedChildren.last))
  }

  protected def visitCount(expr: Count,
                           parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.count(parsedChildren.head))
  }

  protected def visitCountDistinct(expr: CountDistinct,
                                   parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.countDistinct(parsedChildren.head, parsedChildren.tail: _*))
  }

  protected def visitCovPopulation(expr: CovPopulation,
                                   parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.covar_pop(parsedChildren.head, parsedChildren.last))
  }

  protected def visitCovSample(expr: CovSample,
                               parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.covar_samp(parsedChildren.head, parsedChildren.last))
  }

  protected def visitFirst(expr: First,
                           parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.first(parsedChildren.head, expr.ignoreNulls))
  }

  protected def visitGrouping(expr: Grouping,
                              parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.grouping(parsedChildren.head))
  }

  protected def visitGroupingID(expr: GroupingID,
                                parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.grouping_id(parsedChildren: _*))
  }

  protected def visitKurtosis(expr: Kurtosis,
                              parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.kurtosis(parsedChildren.head))
  }

  protected def visitLast(expr: Last,
                          parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.last(parsedChildren.head, expr.ignoreNulls))
  }

  protected def visitMax(expr: Max,
                         parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.max(parsedChildren.head))
  }

  protected def visitMin(expr: Min,
                         parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.min(parsedChildren.head))
  }

  protected def visitSkewness(expr: Skewness,
                              parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.skewness(parsedChildren.head))
  }

  protected def visitStddevSamp(expr: StddevSamp,
                                parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.stddev_samp(parsedChildren.head))
  }

  protected def visitStddevPop(expr: StddevPop,
                               parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.stddev_pop(parsedChildren.head))
  }

  protected def visitSum(expr: Sum,
                         parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    if (expr.isDistinct) {
      Some(sparkFunctions.sumDistinct(parsedChildren.head))
    } else {
      Some(sparkFunctions.sum(parsedChildren.head))
    }
  }

  protected def visitVarianceSamp(expr: VarianceSamp,
                                  parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.var_samp(parsedChildren.head))
  }

  protected def visitVariancePop(expr: VariancePop,
                                 parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.var_pop(parsedChildren.head))
  }


  //////////////////////////////////////////////////////////////////////////////////////////////
  // Non-aggregate functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  protected def visitCreateArray(expr: CreateArray,
                                 parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.array(parsedChildren: _*))
  }

  protected def visitCreateMap(expr: CreateMap,
                               parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.map(parsedChildren: _*))
  }

  protected def visitCoalesce(expr: Coalesce,
                              parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.coalesce(parsedChildren: _*))
  }

  protected def visitInputFileName(expr: InputFileName,
                                   parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.input_file_name())
  }

  protected def visitMonotonicallyIncreasingID(expr: MonotonicallyIncreasingID,
                                               parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.monotonically_increasing_id())
  }

  protected def visitNaNvl(expr: NaNvl, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.nanvl(parsedChildren.head, parsedChildren.last))
  }

  protected def visitRand(expr: Rand, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.rand(expr.seed))
  }

  protected def visitRandn(expr: Randn, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.randn(expr.seed))
  }

  protected def visitSparkPartitionID(expr: SparkPartitionID,
                                      parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.spark_partition_id())
  }

  protected def visitCreateStruct(expr: CreateStruct,
                                  parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.struct(parsedChildren: _*))
  }

  protected def visitRawExpression(expr: RawExpression,
                                   parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.expr(expr.expr))
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Math functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  protected def visitAbs(expr: Abs, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.abs(parsedChildren.head))
  }

  protected def visitSqrt(expr: Sqrt, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.abs(parsedChildren.head))
  }

  protected def visitAcos(expr: Acos, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.acos(parsedChildren.head))
  }

  protected def visitAsin(expr: Asin, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.asin(parsedChildren.head))
  }

  protected def visitAtan(expr: Atan, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.atan(parsedChildren.head))
  }

  protected def visitAtan2(expr: Atan2, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.atan2(parsedChildren.head, parsedChildren.last))
  }

  protected def visitBin(expr: Bin, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.bin(parsedChildren.head))
  }

  protected def visitCbrt(expr: Cbrt, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.cbrt(parsedChildren.head))
  }

  protected def visitCeil(expr: Ceil, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.ceil(parsedChildren.head))
  }

  protected def visitConv(expr: Conv, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.conv(parsedChildren.head, expr.fromBase, expr.toBase))
  }

  protected def visitCos(expr: Cos, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.cos(parsedChildren.head))
  }

  protected def visitCosh(expr: Cosh, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.cosh(parsedChildren.head))
  }

  protected def visitExp(expr: Exp, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.exp(parsedChildren.head))
  }

  protected def visitExpm1(expr: Expm1, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.expm1(parsedChildren.head))
  }

  protected def visitFactorial(expr: Factorial, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.factorial(parsedChildren.head))
  }

  protected def visitFloor(expr: Floor, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.floor(parsedChildren.head))
  }

  protected def visitGreatest(expr: Greatest, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.greatest(parsedChildren: _*))
  }

  protected def visitHex(expr: Hex, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.hex(parsedChildren.head))
  }

  protected def visitUnhex(expr: Unhex, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.unhex(parsedChildren.head))
  }

  protected def visitHypot(expr: Hypot, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.hypot(parsedChildren.head, parsedChildren.last))
  }

  protected def visitLeast(expr: Least, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.least(parsedChildren: _*))
  }

  protected def visitLog(expr: Log, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.log(parsedChildren.head))
  }

  protected def visitLogarithm(expr: Logarithm, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.log(expr.base, parsedChildren.head))
  }

  protected def visitLog10(expr: Log10, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.log10(parsedChildren.head))
  }

  protected def visitLog1p(expr: Log1p, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.log1p(parsedChildren.head))
  }

  protected def visitLog2(expr: Log2, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.log2(parsedChildren.head))
  }

  protected def visitPow(expr: Pow, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.pow(parsedChildren.head, parsedChildren.last))
  }

  protected def visitPmod(expr: Pmod, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.pmod(parsedChildren.head, parsedChildren.last))
  }

  protected def visitRint(expr: Rint, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.rint(parsedChildren.head))
  }

  protected def visitRound(expr: Round, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.round(parsedChildren.head))
  }

  protected def visitBRound(expr: BRound, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.bround(parsedChildren.head, expr.scale))
  }

  protected def visitShiftLeft(expr: ShiftLeft, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.shiftLeft(parsedChildren.head, expr.numBits))
  }

  protected def visitShiftRight(expr: ShiftRight, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.shiftRight(parsedChildren.head, expr.numBits))
  }

  protected def visitShiftRightUnsigned(expr: ShiftRightUnsigned,
                                        parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.shiftRightUnsigned(parsedChildren.head, expr.numBits))
  }

  protected def visitSignum(expr: Signum, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.signum(parsedChildren.head))
  }

  protected def visitSin(expr: Sin, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.sin(parsedChildren.head))
  }

  protected def visitSinh(expr: Sinh, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.sinh(parsedChildren.head))
  }

  protected def visitTan(expr: Tan, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.tan(parsedChildren.head))
  }

  protected def visitTanh(expr: Tanh, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.tanh(parsedChildren.head))
  }

  protected def visitToDegrees(expr: ToDegrees, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.toDegrees(parsedChildren.head))
  }

  protected def visitToRadians(expr: ToRadians, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.toRadians(parsedChildren.head))
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Misc functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  protected def visitMd5(expr: Md5, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.md5(parsedChildren.head))
  }

  protected def visitSha1(expr: Sha1, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.sha1(parsedChildren.head))
  }

  protected def visitSha2(expr: Sha2, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.sha2(parsedChildren.head, expr.numBits))
  }

  protected def visitCrc32(expr: Crc32, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.crc32(parsedChildren.head))
  }

  protected def visitMurmur3Hash(expr: Murmur3Hash, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.hash(parsedChildren: _*))
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  // String functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  protected def visitAscii(expr: Ascii, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.ascii(parsedChildren.head))
  }

  protected def visitBase64(expr: Base64, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.base64(parsedChildren.head))
  }

  protected def visitConcat(expr: Concat, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.concat(parsedChildren: _*))
  }

  protected def visitConcatWs(expr: ConcatWs, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.concat_ws(expr.sep, parsedChildren: _*))
  }

  protected def visitDecode(expr: Decode, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.decode(parsedChildren.head, expr.charset))
  }

  protected def visitEncode(expr: Encode, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.encode(parsedChildren.head, expr.charset))
  }

  protected def visitFormatNumber(expr: FormatNumber, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.format_number(parsedChildren.head, expr.precision))
  }

  protected def visitFormatString(expr: FormatString, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.format_string(expr.format, parsedChildren: _*))
  }

  protected def visitInitCap(expr: InitCap, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.initcap(parsedChildren.head))
  }

  protected def visitStringInstr(expr: StringInstr, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.instr(parsedChildren.head, expr.subStr))
  }

  protected def visitLength(expr: Length, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.length(parsedChildren.head))
  }

  protected def visitLower(expr: Lower, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.lower(parsedChildren.head))
  }

  protected def visitLevenshtein(expr: Levenshtein, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.levenshtein(parsedChildren.head, parsedChildren.last))
  }

  protected def visitStringLocate(expr: StringLocate, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.locate(expr.substr, parsedChildren.head, expr.pos))
  }

  protected def visitStringLPad(expr: StringLPad, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.lpad(parsedChildren.head, expr.len, expr.pad))
  }

  protected def visitStringTrimLeft(expr: StringTrimLeft, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.ltrim(parsedChildren.head))
  }

  protected def visitRegExpExtract(expr: RegExpExtract, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.regexp_extract(parsedChildren.head, expr.expr, expr.groupIdx))
  }

  protected def visitRegExpReplace(expr: RegExpReplace, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.regexp_replace(parsedChildren.head, expr.pattern, expr.replacement))
  }

  protected def visitUnBase64(expr: UnBase64, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.unbase64(parsedChildren.head))
  }

  protected def visitStringRPad(expr: StringRPad, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.rpad(parsedChildren.head, expr.len, expr.pad))
  }

  protected def visitStringRepeat(expr: StringRepeat, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.repeat(parsedChildren.head, expr.n))
  }

  protected def visitStringReverse(expr: StringReverse, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.reverse(parsedChildren.head))
  }

  protected def visitStringTrimRight(expr: StringTrimRight, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.rtrim(parsedChildren.head))
  }

  protected def visitSoundEx(expr: SoundEx, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.soundex(parsedChildren.head))
  }

  protected def visitStringSplit(expr: StringSplit, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.split(parsedChildren.head, expr.pattern))
  }

  protected def visitSubstring(expr: Substring, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.substring(parsedChildren.head, expr.pos, expr.len))
  }

  protected def visitSubstringIndex(expr: SubstringIndex, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.substring_index(parsedChildren.head, expr.delim, expr.count))
  }

  protected def visitStringTranslate(expr: StringTranslate, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.translate(parsedChildren.head, expr.matching, expr.replace))
  }

  protected def visitStringTrim(expr: StringTrim, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.trim(parsedChildren.head))
  }

  protected def visitUpper(expr: Upper, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.upper(parsedChildren.head))
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  // DateTime functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  protected def visitAddMonths(expr: AddMonths, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.add_months(parsedChildren.head, expr.numMonths))
  }

  protected def visitCurrentDate(expr: CurrentDate, parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.current_date())
  }

  protected def visitCurrentTimestamp(expr: CurrentTimestamp,
                                      parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.current_timestamp())
  }

  protected def visitDateFormatClass(expr: DateFormatClass,
                                     parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.date_format(parsedChildren.head, expr.format))
  }

  protected def visitDateAdd(expr: DateAdd,
                             parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.date_add(parsedChildren.head, expr.days))
  }

  protected def visitDateSub(expr: DateSub,
                             parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.date_sub(parsedChildren.head, expr.days))
  }

  protected def visitDateDiff(expr: DateDiff,
                              parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.datediff(parsedChildren.head, parsedChildren.last))
  }

  protected def visitYear(expr: Year,
                          parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.year(parsedChildren.head))
  }

  protected def visitQuarter(expr: Quarter,
                             parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.quarter(parsedChildren.head))
  }

  protected def visitMonth(expr: Month,
                           parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.month(parsedChildren.head))
  }

  protected def visitDayOfMonth(expr: DayOfMonth,
                                parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.dayofmonth(parsedChildren.head))
  }

  protected def visitDayOfYear(expr: DayOfYear,
                               parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.dayofyear(parsedChildren.head))
  }

  protected def visitHour(expr: Hour,
                          parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.hour(parsedChildren.head))
  }

  protected def visitLastDay(expr: LastDay,
                             parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.last_day(parsedChildren.head))
  }

  protected def visitMinute(expr: Minute,
                            parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.minute(parsedChildren.head))
  }

  protected def visitMonthsBetween(expr: MonthsBetween,
                                   parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.months_between(parsedChildren.head, parsedChildren.last))
  }

  protected def visitNextDay(expr: NextDay,
                             parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.next_day(parsedChildren.head, expr.dayOfWeek))
  }

  protected def visitSecond(expr: Second,
                            parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.second(parsedChildren.head))
  }

  protected def visitWeekOfYear(expr: WeekOfYear,
                                parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.weekofyear(parsedChildren.head))
  }

  protected def visitFromUnixTime(expr: FromUnixTime,
                                  parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.from_unixtime(parsedChildren.head, expr.format))
  }

  protected def visitUnixTimestamp(expr: UnixTimestamp,
                                   parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.unix_timestamp(parsedChildren.head, expr.format))
  }

  protected def visitToDate(expr: ToDate,
                            parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.to_date(parsedChildren.head))
  }

  protected def visitTruncDate(expr: TruncDate,
                               parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.unix_timestamp(parsedChildren.head, expr.format))
  }

  protected def visitFromUTCTimestamp(expr: FromUTCTimestamp,
                                      parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.from_utc_timestamp(parsedChildren.head, expr.tz))
  }

  protected def visitToUTCTimestamp(expr: ToUTCTimestamp,
                                    parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.to_utc_timestamp(parsedChildren.head, expr.tz))
  }

  protected def visitTimeWindow(expr: TimeWindow,
                                parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.window(parsedChildren.head, expr.windowDuration, expr.slideDuration, expr.startTime))
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Collection functions
  //////////////////////////////////////////////////////////////////////////////////////////////

  protected def visitArrayContains(expr: ArrayContains,
                                   parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.array_contains(parsedChildren.head, parsedChildren.last))
  }

  protected def visitExplode(expr: Explode,
                             parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.explode(parsedChildren.head))
  }

  protected def visitPosExplode(expr: PosExplode,
                                parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.posexplode(parsedChildren.head))
  }

  protected def visitGetJsonObject(expr: GetJsonObject,
                                   parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.get_json_object(parsedChildren.head, expr.path))
  }

  protected def visitJsonTuple(expr: JsonTuple,
                               parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.json_tuple(parsedChildren.head, expr.fields: _*))
  }

  protected def visitSize(expr: Size,
                          parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.size(parsedChildren.head))
  }

  protected def visitSortArray(expr: SortArray,
                               parsedChildren: Seq[SparkColumn]): Option[SparkColumn] = {
    Some(sparkFunctions.sort_array(parsedChildren.head, expr.asc))
  }
}
