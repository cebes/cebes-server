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

import io.cebes.df.Column
import io.cebes.df.expressions.Expression
import io.cebes.server.routes.HttpJsonProtocol
import io.cebes.spark.df.expressions.SparkPrimitiveExpression
import spray.json._


case class DataframeRequest(df: UUID)

case class LimitRequest(df: UUID, n: Int)

case class SampleRequest(df: UUID, withReplacement: Boolean, fraction: Double, seed: Long)

case class ColumnNamesRequest(df: UUID, columns: Array[String])

case class ColumnsRequest(df: UUID, columns: Array[Column])

case class WithColumnRequest(df: UUID, colName: String, column: Column)

case class WithColumnRenamedRequest(df: UUID, existingName: String, newName: String)

case class AliasRequest(df: UUID, alias: String)

case class JoinRequest(leftDf: UUID, rightDf: UUID, joinExprs: Column, joinType: String)

case class DataframeSetRequest(df: UUID, otherDf: UUID)

case class DropNARequest(df: UUID, minNonNulls: Int, cols: Array[String])

case class FillNARequest(df: UUID, value: Either[String, Double], cols: Array[String])

case class FillNAWithMapRequest(df: UUID, valueMap: Map[String, Any])

case class ReplaceRequest(df: UUID, cols: Array[String], replacement: Map[Any, Any])

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
      val jsValues = obj.valueMap.map {
        case (key: String, value: Any) =>
          JsArray(key.toJson, writeJson(value))
      }.toSeq
      JsObject(Map("df" -> obj.df.toJson, "valueMap" -> JsArray(jsValues: _*)))
    }

    override def read(json: JsValue): FillNAWithMapRequest = json match {
      case jsObj: JsObject =>
        val df = jsObj.fields("df").convertTo[UUID]
        val valueMap = jsObj.fields("valueMap") match {
          case arr: JsArray =>
            arr.elements.map {
              case element: JsArray =>
                require(element.elements.length == 2, "Require a JsArray of length 2, for the key and value")
                element.elements.head.convertTo[String] -> readJson(element.elements.last)
              case other =>
                deserializationError(s"Require an array of the (key, value) pairs, got ${other.compactPrint}")
            }.toMap
          case other =>
            deserializationError(s"Expected a JsArray, got ${other.compactPrint}")
        }
        FillNAWithMapRequest(df, valueMap)
      case other =>
        deserializationError(s"Expected a JsObject, got ${other.compactPrint}")
    }
  }

  implicit object ReplaceRequestFormat extends RootJsonFormat[ReplaceRequest] {

    override def write(obj: ReplaceRequest): JsValue = {
      val jsMap = obj.replacement.map {
        case (k, v) => JsArray(writeJson(k), writeJson(v))
      }.toSeq
      JsObject(Map("df" -> obj.df.toJson,
        "cols" -> obj.cols.toJson,
        "replacement" -> JsArray(jsMap: _*)))
    }

    override def read(json: JsValue): ReplaceRequest = json match {
      case jsObj: JsObject =>
        val df = jsObj.fields("df").convertTo[UUID]
        val cols = jsObj.fields("cols").convertTo[Array[String]]
        val replacement = jsObj.fields("replacement") match {
          case arr: JsArray => arr.elements.map {
            case element: JsArray =>
              require(element.elements.length == 2, "Require a JsArray of length 2, for the key and value")
              readJson(element.elements.head) -> readJson(element.elements.last)
            case other =>
              deserializationError(s"Expected a JsArray, got ${other.compactPrint}")
          }.toMap
          case other =>
            deserializationError(s"Expected a JsArray, got ${other.compactPrint}")
        }
        ReplaceRequest(df, cols, replacement)
      case other =>
        deserializationError(s"Expected a JsObject, got ${other.compactPrint}")
    }
  }
}

object HttpDfJsonProtocol extends HttpDfJsonProtocol
