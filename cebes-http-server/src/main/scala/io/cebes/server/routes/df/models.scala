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

case class SampleRequest(df: UUID, withReplacement: Boolean, fraction: Double, seed: Long)

case class TakeRequest(df: UUID, n: Int)

case class ColumnsRequest(df: UUID, columns: Array[String])

case class CountRequest(df: UUID)

trait HttpDfJsonProtocol extends HttpJsonProtocol {

  implicit val sampleRequestFormat = jsonFormat4(SampleRequest)
  implicit val takeRequestFormat = jsonFormat2(TakeRequest)
  implicit val columnsRequestFormat = jsonFormat2(ColumnsRequest)
  implicit val countRequestFormat = jsonFormat1(CountRequest)

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
}

object HttpDfJsonProtocol extends HttpDfJsonProtocol
