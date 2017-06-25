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
 */

package io.cebes.pipeline.json

import io.cebes.json.CebesCoreJsonProtocol._
import io.cebes.json.CebesExpressionJsonProtocol
import spray.json.DefaultJsonProtocol._
import spray.json._

trait PipelineJsonProtocol extends CebesExpressionJsonProtocol {

  implicit object ValueDefFormat extends JsonFormat[ValueDef] {

    override def write(obj: ValueDef): JsValue = writeJson(obj.value)

    override def read(json: JsValue): ValueDef = ValueDef(readJson(json))
  }

  implicit val stageOutputDefFormat: RootJsonFormat[StageOutputDef] = jsonFormat2(StageOutputDef)
  implicit val dataframeMessageDefFormat: RootJsonFormat[DataframeMessageDef] = jsonFormat1(DataframeMessageDef)
  implicit val sampleMessageDefFormat: RootJsonFormat[SampleMessageDef] = jsonFormat0(SampleMessageDef)
  implicit val modelMessageDefFormat: RootJsonFormat[ModelMessageDef] = jsonFormat0(ModelMessageDef)
  implicit val columnDefFormat: RootJsonFormat[ColumnDef] = jsonFormat1(ColumnDef)

  implicit object PipelineMessageDefFormat extends JsonFormat[PipelineMessageDef] {

    override def write(obj: PipelineMessageDef): JsValue = obj match {
      case value: ValueDef => JsArray(JsString("ValueDef"), value.toJson)
      case stageOutput: StageOutputDef => JsArray(JsString("StageOutputDef"), stageOutput.toJson)
      case dfMsg: DataframeMessageDef => JsArray(JsString("DataframeMessageDef"), dfMsg.toJson)
      case sampleMsg: SampleMessageDef => JsArray(JsString("SampleMessageDef"), sampleMsg.toJson)
      case modelMsg: ModelMessageDef => JsArray(JsString("ModelMessageDef"), modelMsg.toJson)
      case colDef: ColumnDef => JsArray(JsString("ColumnDef"), colDef.toJson)
      case other => serializationError(s"Couldn't serialize type ${other.getClass.getCanonicalName}")
    }

    override def read(json: JsValue): PipelineMessageDef = json match {
      case jsArr: JsArray =>
        require(jsArr.elements.size == 2, "Expected a JsArray of 2 elements")
        jsArr.elements.head match {
          case JsString("ValueDef") => jsArr.elements.last.convertTo[ValueDef]
          case JsString("StageOutputDef") => jsArr.elements.last.convertTo[StageOutputDef]
          case JsString("DataframeMessageDef") => jsArr.elements.last.convertTo[DataframeMessageDef]
          case JsString("SampleMessageDef") => jsArr.elements.last.convertTo[SampleMessageDef]
          case JsString("ModelMessageDef") => jsArr.elements.last.convertTo[ModelMessageDef]
          case JsString("ColumnDef") => jsArr.elements.last.convertTo[ColumnDef]
          case _ =>
            deserializationError(s"Unable to deserialize ${jsArr.compactPrint}")
        }
      case other =>
        deserializationError(s"Expected a JsArray, got ${other.getClass.getCanonicalName}")
    }
  }

  implicit val stageDefFormat: RootJsonFormat[StageDef] = jsonFormat4(StageDef)
  implicit val modelDefFormat: RootJsonFormat[ModelDef] = jsonFormat4(ModelDef)
  implicit val pipelineDefFormat: RootJsonFormat[PipelineDef] = jsonFormat2(PipelineDef)
  implicit val pipelineRunDefFormat: RootJsonFormat[PipelineRunDef] = jsonFormat4(PipelineRunDef)
}

private[json] object PipelineDefaultJsonProtocol extends PipelineJsonProtocol
