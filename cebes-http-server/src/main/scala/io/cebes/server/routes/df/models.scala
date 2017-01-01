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
 * Created by phvu on 13/12/2016.
 */

package io.cebes.server.routes.df

import java.util.UUID

import io.cebes.common.Tag
import io.cebes.df.Column
import io.cebes.df.DataframeService.AggregationTypes
import io.cebes.df.expressions.Expression
import io.cebes.df.types.VariableTypes.VariableType
import io.cebes.server.routes.HttpJsonProtocol
import io.cebes.spark.df.expressions.SparkPrimitiveExpression
import spray.json._

import scala.util.{Failure, Success, Try}

case class DataframeRequest(df: UUID)

case class WithVariableTypesRequest(df: UUID, variableTypes: Map[String, VariableType])

case class LimitRequest(df: UUID, n: Int)

case class SampleRequest(df: UUID, withReplacement: Boolean, fraction: Double, seed: Long)

case class ColumnNamesRequest(df: UUID, colNames: Array[String])

case class ColumnsRequest(df: UUID, cols: Array[Column])

case class WithColumnRequest(df: UUID, colName: String, col: Column)

case class WithColumnRenamedRequest(df: UUID, existingName: String, newName: String)

case class AliasRequest(df: UUID, alias: String)

case class JoinRequest(leftDf: UUID, rightDf: UUID, joinExprs: Column, joinType: String)

case class DataframeSetRequest(df: UUID, otherDf: UUID)

case class DropNARequest(df: UUID, minNonNulls: Int, colNames: Array[String])

case class FillNARequest(df: UUID, value: Either[String, Double], colNames: Array[String])

case class FillNAWithMapRequest(df: UUID, valueMap: Map[String, Any])

case class ReplaceRequest(df: UUID, colNames: Array[String], replacement: Map[Any, Any])

case class ApproxQuantileRequest(df: UUID, colName: String, probabilities: Array[Double], relativeError: Double)

case class FreqItemsRequest(df: UUID, colNames: Array[String], support: Double)

case class SampleByRequest(df: UUID, colName: String, fractions: Map[Any, Double], seed: Long)

case class TagAddRequest(tag: Tag, df: UUID)

case class TagDeleteRequest(tag: Tag)

case class TagsGetRequest(pattern: Option[String], maxCount: Int = 100)

/**
  * Perform aggregation on the [[io.cebes.df.Dataframe]] of the given ID.
  * The aggregation can be `groupBy`, `rollup` or `cube`, depending on `aggType`.
  * The aggregation will be performed on the columns given in `cols`, or on none column if `cols` is empty.
  *
  * Optionally, also perform a pivot on column `pivotColName`, if it is provided.
  * `pivotValues` contains the values of `pivotColName` to be pivoted on.
  * If `pivotValues` is not provided, it will be computed from the data (which is less efficient).
  *
  * After the aggregation (and pivot, if needed), perform some actual computation:
  *  - If `genericAggExprs` is provided: perform the computation specified in this argument
  *  - Otherwise, compute the aggregation functions specified in `aggFunc`.
  * Supported values of `aggFunc` are: count, min, mean, max, sum.
  * `aggFunc` will only be applied on columns specified in `aggColNames`, otherwise it will be applied on
  * all numeric non-aggregate columns.
  */
case class AggregateRequest(df: UUID, cols: Array[Column], aggType: AggregationTypes.AggregationType,
                            pivotColName: Option[String], pivotValues: Option[Array[Any]],
                            genericAggExprs: Option[Array[Column]],
                            aggFunc: Option[String],
                            aggColNames: Array[String])


trait HttpDfJsonProtocol extends HttpJsonProtocol {

  implicit object SparkExpressionFormat extends AbstractExpressionFormat {

    override protected def writeExpression(expr: Expression): Option[JsValue] = expr match {
      case s: SparkPrimitiveExpression =>
        Some(JsObject(Map("SparkPrimitiveExpression" -> JsTrue,
          "dfId" -> s.dfId.toJson,
          "colName" -> JsString(s.colName))))
      case _ => None
    }

    override protected def readExpression(json: JsValue): Option[Expression] = {
      json match {
        case jsObj: JsObject =>
          jsObj.fields.get("SparkPrimitiveExpression") match {
            case Some(JsTrue) =>
              Some(SparkPrimitiveExpression(jsObj.fields("dfId").convertTo[UUID],
                jsObj.fields("colName").convertTo[String], None))
            case _ => None
          }
        case _ => None
      }
    }
  }

  implicit object ColumnFormat extends JsonFormat[Column] {

    override def write(obj: Column): JsValue = {
      JsObject(Map("expr" -> obj.expr.toJson))
    }

    override def read(json: JsValue): Column = json match {
      case jsObj: JsObject =>
        jsObj.fields.get("expr") match {
          case Some(obj) => new Column(obj.convertTo[Expression])
          case _ => deserializationError("Expression must be provided in key 'expr'")
        }
      case _ =>
        deserializationError("A JsObject is expected")
    }
  }

  implicit val dataframeRequestFormat = jsonFormat1(DataframeRequest)
  implicit val withVariableTypesRequestFormat = jsonFormat2(WithVariableTypesRequest)
  implicit val limitRequestFormat = jsonFormat2(LimitRequest)
  implicit val sampleRequestFormat = jsonFormat4(SampleRequest)
  implicit val columnNamesRequestFormat = jsonFormat2(ColumnNamesRequest)
  implicit val columnsRequestFormat = jsonFormat2(ColumnsRequest)
  implicit val withColumnRequestFormat = jsonFormat3(WithColumnRequest)
  implicit val withColumnRenamedRequestFormat = jsonFormat3(WithColumnRenamedRequest)
  implicit val aliasRequestFormat = jsonFormat2(AliasRequest)
  implicit val joinRequestFormat = jsonFormat4(JoinRequest)
  implicit val dataframeSetRequestFormat = jsonFormat2(DataframeSetRequest)

  implicit val dropNARequestFormat = jsonFormat3(DropNARequest)
  implicit val fillNARequestFormat = jsonFormat3(FillNARequest)

  implicit object FillNAWithMapRequestFormat extends RootJsonFormat[FillNAWithMapRequest] {

    override def write(obj: FillNAWithMapRequest): JsValue = {
      JsObject(Map("df" -> obj.df.toJson, "valueMap" -> writeMap(obj.valueMap)))
    }

    override def read(json: JsValue): FillNAWithMapRequest = json match {
      case jsObj: JsObject =>
        val df = jsObj.fields("df").convertTo[UUID]
        val valueMap = readMap[String, Any](jsObj.fields("valueMap"))
        FillNAWithMapRequest(df, valueMap)
      case other =>
        deserializationError(s"Expected a JsObject, got ${other.compactPrint}")
    }
  }

  implicit object ReplaceRequestFormat extends RootJsonFormat[ReplaceRequest] {

    override def write(obj: ReplaceRequest): JsValue = {
      JsObject(Map("df" -> obj.df.toJson,
        "cols" -> obj.colNames.toJson,
        "replacement" -> writeMap(obj.replacement)))
    }

    override def read(json: JsValue): ReplaceRequest = json match {
      case jsObj: JsObject =>
        val df = jsObj.fields("df").convertTo[UUID]
        val cols = jsObj.fields("cols").convertTo[Array[String]]
        val replacement = readMap[Any, Any](jsObj.fields("replacement"))
        ReplaceRequest(df, cols, replacement)
      case other =>
        deserializationError(s"Expected a JsObject, got ${other.compactPrint}")
    }
  }

  implicit val approxQuantileRequestFormat = jsonFormat4(ApproxQuantileRequest)
  implicit val freqItemsRequestFormat = jsonFormat3(FreqItemsRequest)

  implicit object SampleByRequestFormat extends RootJsonFormat[SampleByRequest] {

    override def write(obj: SampleByRequest): JsValue = {
      JsObject(Map("df" -> obj.df.toJson,
        "col" -> obj.colName.toJson,
        "fractions" -> writeMap(obj.fractions),
        "seed" -> obj.seed.toJson))
    }

    override def read(json: JsValue): SampleByRequest = json match {
      case jsObj: JsObject =>
        val df = jsObj.fields("df").convertTo[UUID]
        val col = jsObj.fields("col").convertTo[String]
        val fractions = readMap[Any, Double](jsObj.fields("fractions"))
        val seed = jsObj.fields("seed").convertTo[Long]
        SampleByRequest(df, col, fractions, seed)
      case other =>
        deserializationError(s"Expected a JsObject, got ${other.compactPrint}")
    }
  }

  implicit object AggregationTypeFormat extends JsonFormat[AggregationTypes.AggregationType] {
    override def write(obj: AggregationTypes.AggregationType): JsValue = obj.name.toJson

    override def read(json: JsValue): AggregationTypes.AggregationType = json.convertTo[String] match {
      case AggregationTypes.GroupBy.name => AggregationTypes.GroupBy
      case AggregationTypes.RollUp.name => AggregationTypes.RollUp
      case AggregationTypes.Cube.name => AggregationTypes.Cube
      case v => deserializationError(s"Unrecognized aggregation type: $v")
    }
  }

  implicit object AggregateRequestFormat extends RootJsonFormat[AggregateRequest] {

    override def write(obj: AggregateRequest): JsValue = {
      val pivotValueJs = obj.pivotValues match {
        case None => JsNull
        case Some(values) => JsArray(values.map(writeJson): _*)
      }
      JsObject(Map(
        "df" -> obj.df.toJson,
        "cols" -> obj.cols.toJson,
        "aggType" -> obj.aggType.toJson,
        "pivotColName" -> obj.pivotColName.toJson,
        "pivotValues" -> pivotValueJs,
        "genericAggExprs" -> obj.genericAggExprs.toJson,
        "aggFunc" -> obj.aggFunc.toJson,
        "aggColNames" -> obj.aggColNames.toJson
      ))
    }

    override def read(json: JsValue): AggregateRequest = json match {
      case jsObj: JsObject =>
        val pivotValues = jsObj.fields("pivotValues") match {
          case JsNull => None
          case arr: JsArray => Some(arr.elements.map(readJson).toArray)
          case other =>
            deserializationError(s"Expected some pivotValues as JsArray or JsNull, got ${other.compactPrint}")
        }

        AggregateRequest(
          df = jsObj.fields("df").convertTo[UUID],
          cols = jsObj.fields("cols").convertTo[Array[Column]],
          aggType = jsObj.fields("aggType").convertTo[AggregationTypes.AggregationType],
          pivotColName = safeReadJs[String](jsObj.fields.get("pivotColName")),
          pivotValues = pivotValues,
          genericAggExprs = safeReadJs[Array[Column]](jsObj.fields.get("genericAggExprs")),
          aggFunc = safeReadJs[String](jsObj.fields.get("aggFunc")),
          aggColNames = jsObj.fields("aggColNames").convertTo[Array[String]]
        )
      case other => deserializationError(s"Expected a JsObject, got ${other.compactPrint}")
    }

    private def safeReadJs[T](json: Option[JsValue])(implicit jrT: JsonReader[T]): Option[T] = json.flatMap {
      case JsNull => None
      case v => Some(v.convertTo[T])
    }
  }

  implicit object TagFormat extends RootJsonFormat[Tag] {
    override def write(obj: Tag): JsValue = JsString(obj.toString)

    override def read(json: JsValue): Tag = json match {
      case jsStr: JsString =>
        Try(Tag.fromString(jsStr.value)) match {
          case Success(t) => t
          case Failure(f) => deserializationError(s"Failed to parse tag: ${f.getMessage}", f)
        }
      case other =>
        deserializationError(s"Expected a JsString, got ${other.compactPrint}")
    }
  }

  implicit val tagAddRequestFormat = jsonFormat2(TagAddRequest)
  implicit val tagDeleteRequestFormat = jsonFormat1(TagDeleteRequest)
  implicit val tagsGetRequestFormat = jsonFormat2(TagsGetRequest)
}

object HttpDfJsonProtocol extends HttpDfJsonProtocol
