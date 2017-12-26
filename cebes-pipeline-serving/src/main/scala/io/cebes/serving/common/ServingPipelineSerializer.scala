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
package io.cebes.serving.common

import com.google.inject.Inject
import io.cebes.df.{Column, Dataframe, DataframeService}
import io.cebes.pipeline.ModelService
import io.cebes.pipeline.ml.Model
import io.cebes.pipeline.models._
import io.cebes.serving.common.DefaultPipelineJsonProtocol._
import spray.json.DefaultJsonProtocol._
import spray.json._

/**
  * Serializer for cebes-serving
  */
class ServingPipelineSerializer @Inject()(private val dfService: DataframeService,
                                          private val modelService: ModelService) {

  /**
    * Serialize the given value into a [[JsValue]]
    */
  def serialize(value: Any, maxDfSize: Int): JsValue = {
    value match {
      case df: Dataframe =>
        if (maxDfSize < 0) {
          // return the Dataframe ID
          JsObject(Map("dfId" -> df.id.toString.toJson))
        } else {
          // serialize maximum maxDfSize rows of the dataframe into JSON
          dfService.serialize(df.limit(maxDfSize))
        }
      case model: Model => JsObject(Map("modelId" -> model.id.toString.toJson))
      case col: Column => col.toJson
      case slot: SlotDescriptor => JsObject(Map("stageName" -> slot.parent.toJson, "outputName" -> slot.name.toJson))

      case null => JsNull
      case true => JsTrue
      case false => JsFalse
      case s: String => JsString(s)
      case num: Number => JsNumber(num.doubleValue())
      case arr: Array[_] => JsArray(arr.map(v => serialize(v, maxDfSize)).toVector)
      case other =>
        serializationError(s"Do not known how to serialize value of type ${other.getClass.getName}: ${other.toString}")
    }
  }

  /**
    * Deserialize the given [[JsValue]] into a value to be fed into the given [[Pipeline]] at the given slot
    */
  def deserialize(pipeline: Pipeline, slotDesc: SlotDescriptor, jsValue: JsValue): Any = {
    jsValue match {
      case JsNull => null
      case JsTrue => true
      case JsFalse => false
      case s: JsString => s.value
      case num: JsNumber =>
        val converters = Map(
          classOf[BigDecimal] -> ((d: BigDecimal) => d),
          classOf[BigInt] -> ((d: BigDecimal) => d.toBigInt()),
          classOf[Double] -> ((d: BigDecimal) => d.toDouble),
          classOf[Float] -> ((d: BigDecimal) => d.toFloat),
          classOf[Long] -> ((d: BigDecimal) => d.toLong),
          classOf[Int] -> ((d: BigDecimal) => d.toInt),
          classOf[Short] -> ((d: BigDecimal) => d.toShort),
          classOf[Byte] -> ((d: BigDecimal) => d.toByte)
        )
        val slotType = safeGetSlot(pipeline, slotDesc).messageClass
        converters.get(slotType) match {
          case Some(f) => f(num.value)
          case None =>
            deserializationError(s"Cannot deserialize value ${jsValue.toString()} into type ${slotType.toString}")
        }
      case arr: JsArray => arr.elements.map(v => deserialize(pipeline, slotDesc, v)).toArray
      case jsObj: JsObject =>

        if (jsObj.fields.size == 2 && jsObj.fields.contains("stageName") && jsObj.fields.contains("outputName")) {

          // when jsObj is a stage output
          val stageName = jsObj.fields("stageName").convertTo[String]
          val outputName = jsObj.fields("outputName").convertTo[String]
          safeGetSlot(pipeline, SlotDescriptor(stageName, outputName)) match {
            case out: OutputSlot[_] => out
            case _ =>
              deserializationError(s"No output slot matches ${jsObj.toString()}")
          }
        } else {
          val slot = safeGetSlot(pipeline, slotDesc)
          slot.messageClass match {
            case dfClass if classOf[Dataframe].isAssignableFrom(dfClass) =>
              if (jsObj.fields.size == 1 && jsObj.fields.contains("dfId")) {
                dfService.get(jsObj.fields("dfId").convertTo[String])
              } else {
                dfService.deserialize(jsObj)
              }
            case modelClass if classOf[Model].isAssignableFrom(modelClass) =>
              require(jsObj.fields.contains("modelId"))
              modelService.get(jsObj.fields("modelId").convertTo[String])
            case colClass if classOf[Column].isAssignableFrom(colClass) =>
              jsObj.convertTo[Column]
            case other =>
              deserializationError(s"Could not deserialize value ${jsObj.toString()} " +
                s"into slot ${slotDesc.toString} of type ${other.getClass.getName}")
          }
        }

    }
  }


  private def safeGetSlot(pipeline: Pipeline, slotDesc: SlotDescriptor): Slot[Any] = {
    pipeline.stages.get(slotDesc.parent).flatMap { s => s.getSlot(slotDesc.name) } match {
      case Some(s) => s
      case None =>
        throw new IllegalArgumentException(s"Slot ${slotDesc.parent}:${slotDesc.name} can not" +
          s" be found in pipeline ID ${pipeline.id.toString}")
    }
  }
}
