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

import java.util.UUID

import io.cebes.df.{Column, Dataframe}
import io.cebes.pipeline.json._
import io.cebes.pipeline.ml.Model

/**
  * Functions for serializing and deserializing [[PipelineMessageDef]]
  */
trait PipelineMessageSerializer {

  /**
    * Get a [[Dataframe]] object given the ID.
    * This should be implemented by child classes
    */
  protected def getDataframe(dfId: UUID): Dataframe

  /**
    * Get a [[Model]] object given the ID
    * Should be implemented by child classes
    */
  protected def getModel(modelId: UUID): Model

  //////////////////////////////////////////////////
  // private helper
  //////////////////////////////////////////////////

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

  //////////////////////////////////////////////////
  // Public APIs
  //////////////////////////////////////////////////

  /**
    * Serialize the given value into a [[PipelineMessageDef]]
    */
  def serialize[T](value: T): PipelineMessageDef = {
    value match {
      case col: Column => ColumnDef(col)
      case df: Dataframe => DataframeMessageDef(df.id)
      case model: Model => ModelMessageDef(model.id)
      case slot: SlotDescriptor => StageOutputDef(slot.parent, slot.name)
      case other => writeValueDef(other)
    }
  }

  /**
    * Parse the given [[PipelineMessageDef]] and return the actual value, which can be
    * used to feed the pipeline stages
    *
    * This function will NOT set the input if the input message is a [[StageOutputDef]].
    */
  def deserialize(pplMsg: PipelineMessageDef): Any = {
    pplMsg match {
      case v: ValueDef => v.value
      case c: ColumnDef => c.col
      case d: DataframeMessageDef => getDataframe(d.dfId)
      case m: ModelMessageDef => getModel(m.modelId)
      case s: StageOutputDef => SlotDescriptor(s.stageName, s.outputName)
      case valueDef =>
        throw new UnsupportedOperationException(s"Unknown pipeline message of type ${valueDef.getClass.getName}")
    }
  }
}
