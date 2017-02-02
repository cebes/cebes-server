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
package io.cebes.pipeline.models

import io.cebes.df.Column
import io.cebes.json.CebesCoreJsonProtocol._
import io.cebes.pipeline.protos.message.{ColumnDef, PipelineMessageDef}
import io.cebes.pipeline.protos.value.{ArrayDef, MapDef, ScalarDef, ValueDef}
import spray.json._

/**
  * Functions for serializing and deserializing [[PipelineMessageDef]]
  */
object PipelineMessageSerializer {

  /**
    * Serialize the given value into a [[PipelineMessageDef]]
    */
  def serialize[T](value: T, outputSlot: OutputSlot[T]): PipelineMessageDef = {
    if (!outputSlot.messageClass.isAssignableFrom(value.getClass)) {
      throw new IllegalArgumentException(s"Output of type ${outputSlot.messageClass.getSimpleName} " +
        s"but the value is of type ${value.getClass}")
    }
    value match {
      case col: Column =>
        PipelineMessageDef().withColumn(ColumnDef().withColumnJson(col.toJson.compactPrint))
      case other =>
        PipelineMessageDef().withValue(writeValueDef(other))
    }
  }

  private def writeValueDef(v: Any): ValueDef = v match {
    case null =>
      ValueDef()
    case s: String =>
      ValueDef().withScalar(ScalarDef().withStringVal(s))
    case b: Boolean =>
      ValueDef().withScalar(ScalarDef().withBoolVal(b))
    case i: Int =>
      ValueDef().withScalar(ScalarDef().withInt32Val(i))
    case l: Long =>
      ValueDef().withScalar(ScalarDef().withInt64Val(l))
    case f: Float =>
      ValueDef().withScalar(ScalarDef().withFloatVal(f))
    case d: Double =>
      ValueDef().withScalar(ScalarDef().withDoubleVal(d))
    case arr: Array[_] =>
      ValueDef().withArray(ArrayDef(arr.map(writeValueDef)))
    case m: Map[_, _] =>
      ValueDef().withMap(MapDef(m.map { case (k, mapVal) =>
        MapDef.MapEntryDef(Some(writeValueDef(k)), Some(writeValueDef(mapVal)))
      }.toSeq))
    case other =>
      throw new UnsupportedOperationException(s"Do not know how to serialize " +
        s"pipeline message of type ${other.getClass.getName}")
  }

  /**
    * Parse the given protobuf message `inputMsg` and set the value into the input of name `inputName`
    * of the given stage.
    * This function will NOT set the input if the input message is a
    * [[io.cebes.pipeline.protos.message.StageOutputDef]].
    *
    * We need the stage and the input name to correctly parse the input message
    */
  def deserialize(inputMsg: PipelineMessageDef, stage: Stage, inputName: String): Unit = {
    if (!stage.hasInput(inputName)) {
      throw new IllegalArgumentException(s"Input name $inputName not found in stage ${stage.toString}")
    }
    val inpSlot = stage.getInput(inputName)

    // TODO: check if the following input() calls respect the types
    // e.g. check something like inpSlot.messageClass.isAssignableFrom(classOf[String])

    inputMsg.msg match {
      case PipelineMessageDef.Msg.Value(v) =>
        stage.input(inpSlot, PipelineMessageSerializer.readValueDef(v))
      case PipelineMessageDef.Msg.Column(columnDef) =>
        stage.input(inpSlot, columnDef.columnJson.parseJson.convertTo[Column])
      case PipelineMessageDef.Msg.StageOutput(_) =>
      // will be connected later
      case valueDef =>
        throw new UnsupportedOperationException("Unsupported non-scala value for stage parameters: " +
          s"Parameter name ${inpSlot.name} of stage ${stage.toString} " +
          s"with value ${valueDef.toString} of type ${valueDef.getClass.getName}")
    }
  }

  private def readValueDef(v: ValueDef): Any = v.value match {
    case ValueDef.Value.Scalar(scalarDef) =>
      scalarDef.value match {
        case ScalarDef.Value.StringVal(s) => s
        case ScalarDef.Value.BoolVal(b) => b
        case ScalarDef.Value.Int32Val(i) => i
        case ScalarDef.Value.Int64Val(l) => l
        case ScalarDef.Value.FloatVal(f) => f
        case ScalarDef.Value.DoubleVal(d) => d
        case scalarVal =>
          throw new UnsupportedOperationException(s"Unsupported scalar value " +
            s"${scalarVal.toString} of type ${scalarVal.getClass.getName}")
      }
    case ValueDef.Value.Array(arrayDef) =>
      val vals = arrayDef.element.map(readValueDef)
      if (vals.isEmpty) {
        Array[Int]()
      } else {
        vals.head match {
          case _: String => vals.map(_.asInstanceOf[String]).toArray
          case _: Boolean => vals.map(_.asInstanceOf[Boolean]).toArray
          case _: Int => vals.map(_.asInstanceOf[Int]).toArray
          case _: Long => vals.map(_.asInstanceOf[Long]).toArray
          case _: Float => vals.map(_.asInstanceOf[Float]).toArray
          case _: Double => vals.map(_.asInstanceOf[Double]).toArray
          case other =>
            throw new IllegalArgumentException(s"Unsupported array ${vals.toString} " +
              s"of type ${other.getClass.getName}")
        }
      }
    case ValueDef.Value.Map(mapDef) =>
      mapDef.entry.filter(_.key.isDefined).map { entry =>
        readValueDef(entry.key.get) -> entry.value.map(readValueDef).orNull
      }.toMap[Any, Any]
    case ValueDef.Value.Empty =>
      throw new IllegalArgumentException(s"Empty value def: ${v.toString}")
  }
}
