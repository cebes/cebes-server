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
package io.cebes.serving

import io.cebes.pipeline.json.{PipelineJsonProtocol, PipelineMessageDef, SampleMessageDef, ValueDef}
import spray.json.DefaultJsonProtocol._
import spray.json.{RootJsonFormat, _}

case class InferenceRequest(servingName: String,
                            inputs: Map[String, PipelineMessageDef],
                            outputs: Array[String])

case class InferenceResponse(outputs: Map[String, PipelineMessageDef])

case class ServedPipeline(servingName: String,
                          slotNamings: Map[String, String])

case class CebesServingConfiguration(pipelines: Array[ServedPipeline])

////////////////////////////////////////////////////////////////////////////////////////////////
// Json protocols

object DefaultPipelineJsonProtocol extends PipelineJsonProtocol

trait CebesServingJsonProtocol {

  implicit val inferenceRequestFormat: RootJsonFormat[InferenceRequest] = jsonFormat3(InferenceRequest)
  implicit val inferenceResponseFormat: RootJsonFormat[InferenceResponse] = jsonFormat1(InferenceResponse)

  /**
    * A specialized [[JsonFormat]] for [[PipelineMessageDef]]
    * Here we only serialize and deserialize what makes sense for the serving part
    */
  implicit object ServingPipelineMessageDefFormat extends JsonFormat[PipelineMessageDef] {

    override def write(obj: PipelineMessageDef): JsValue = obj match {
      case value: ValueDef =>
        JsArray(JsString("ValueDef"), value.toJson(DefaultPipelineJsonProtocol.ValueDefFormat))
      case sampleMsg: SampleMessageDef =>
        JsArray(JsString("SampleMessageDef"), sampleMsg.toJson(DefaultPipelineJsonProtocol.sampleMessageDefFormat))
      case other => serializationError(s"Couldn't serialize type ${other.getClass.getCanonicalName}")
    }

    override def read(json: JsValue): PipelineMessageDef = json match {
      case jsArr: JsArray =>
        require(jsArr.elements.size == 2, "Expected a JsArray of 2 elements")
        jsArr.elements.head match {
          case JsString("ValueDef") =>
            jsArr.elements.last.convertTo[ValueDef](DefaultPipelineJsonProtocol.ValueDefFormat)
          case JsString("SampleMessageDef") =>
            jsArr.elements.last.convertTo[SampleMessageDef](DefaultPipelineJsonProtocol.sampleMessageDefFormat)
          case _ =>
            deserializationError(s"Unable to deserialize ${jsArr.compactPrint}")
        }
      case other =>
        deserializationError(s"Expected a JsArray, got ${other.getClass.getCanonicalName}")
    }
  }

}

object CebesServingJsonProtocol extends CebesServingJsonProtocol
