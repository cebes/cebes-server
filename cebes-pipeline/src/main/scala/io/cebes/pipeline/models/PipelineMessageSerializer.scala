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

import com.google.inject.Inject
import io.cebes.df.{Column, Dataframe, DataframeService}
import io.cebes.pipeline.json._

/**
  * Functions for serializing and deserializing [[PipelineMessageDef]]
  */
class PipelineMessageSerializer @Inject()(dfService: DataframeService) {

  /**
    * Serialize the given value into a [[PipelineMessageDef]]
    */
  def serialize[T](value: T, outputSlot: OutputSlot[T]): PipelineMessageDef = {
    require(outputSlot.messageClass.isAssignableFrom(value.getClass),
      s"Output of type ${outputSlot.messageClass.getSimpleName} but the value is of type ${value.getClass}")

    value match {
      case col: Column => ColumnDef(col)
      case df: Dataframe =>
        // store the result Dataframe into the cache
        dfService.cache(df)
        DataframeMessageDef(df.id)
      case other => writeValueDef(other)
    }
  }

  /**
    * TODO: This might be a bit overdo, probably a `ValueDef(x)` would be enough for all?
    */
  private def writeValueDef(v: Any): ValueDef = v match {
    case null => ValueDef()
    case s: String => ValueDef(s)
    case b: Boolean => ValueDef(b)
    case i: Int => ValueDef(i)
    case l: Long => ValueDef(l)
    case f: Float => ValueDef(f)
    case d: Double => ValueDef(d)
    case arr: Array[_] => ValueDef(arr)
    case m: Map[_, _] => ValueDef(m)
    case other =>
      throw new UnsupportedOperationException(s"Do not know how to serialize " +
        s"pipeline message of type ${other.getClass.getName}")
  }

  /**
    * Parse the given message `inputMsg` and set the value into the input of name `inputName`
    * of the given stage.
    * This function will NOT set the input if the input message is a [[StageOutputDef]].
    *
    * We need the stage and the input name to correctly parse the input message
    */
  def deserialize(inputMsg: PipelineMessageDef, stage: Stage,
                  inputName: String): Unit = {

    require(stage.hasInput(inputName), s"Input name $inputName not found in stage ${stage.toString}")

    val inpSlot = stage.getInput(inputName)

    // TODO: check if the following input() calls respect the types
    // e.g. check something like inpSlot.messageClass.isAssignableFrom(classOf[String])

    inputMsg match {
      case v: ValueDef => stage.input(inpSlot, v.value)
      case columnDef: ColumnDef => stage.input(inpSlot, columnDef.col)
      case _: StageOutputDef => // will be connected later
      case dfMsgDef: DataframeMessageDef =>
        //val dfService = injector.getInstance(classOf[DataframeService])
        stage.input(inpSlot, dfService.get(dfMsgDef.dfId.toString))
      case valueDef =>
        throw new UnsupportedOperationException("Unsupported non-scala value for stage parameters: " +
          s"Parameter name ${inpSlot.name} of stage ${stage.toString} " +
          s"with value ${valueDef.toString} of type ${valueDef.getClass.getName}")
    }
  }
}
