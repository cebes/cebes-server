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
package io.cebes.pipeline.ml

import io.cebes.common.HasId
import io.cebes.df.Dataframe
import io.cebes.pipeline.models._

/**
  * A ML model, with an ID, a bunch of inputs, and
  * a `transform()` function which transforms an input [[Dataframe]] into another [[Dataframe]].
  */
trait Model extends HasId with Inputs {

  /** Implement this to do the real transformation */
  def transformImpl(data: Dataframe, params: SlotValueMap): Dataframe

  /////////////////////////////////////////////////////////////////////////////
  // public APIs
  /////////////////////////////////////////////////////////////////////////////

  /** Transform the given input [[Dataframe]] to the final Dataframe,
    * using the parameters specified in this Model
    */
  def transform(data: Dataframe): Dataframe = withOrdinaryInputs { params =>
    transformImpl(data, params)
  }

  /**
    * Sets a slot in the embedded slot map.
    * A model can only receive [[OrdinaryInput]] (not to be used in a pipeline)
    */
  override def input[T](slot: InputSlot[T], value: StageInput[T]): this.type = {
    value match {
      case ordinary: OrdinaryInput[_] =>
        super.input(slot, ordinary)
      case _ =>
        throw new IllegalArgumentException("Only ordinary inputs are allowed")
    }
  }

  /**
    * Copy all the input values from the given `slotValueMap` to this Model,
    * if it contains an input slot with the same name
    * Only input slot with the same name (and type!) will be copied. Anything else is ignored.
    *
    * @return this instance
    */
  def copyInputs(slotValueMap: SlotValueMap): this.type = {
    slotValueMap.foreach { case (s: Slot[_], v) if hasInput(s.name) =>
      input(getInput(s.name), v)
    }
    this
  }

  /////////////////////////////////////////////////////////////////////////////
  // Helper
  /////////////////////////////////////////////////////////////////////////////

  private def withOrdinaryInputs[R](work: SlotValueMap => R): R = {
    inputLock.readLock().lock()
    try {
      val inputVals = _inputs.map { slot =>
        val inpValue = input(slot) match {
          case ordinary: OrdinaryInput[_] =>
            ordinary.get
          case _ =>
            throw new IllegalArgumentException("Only ordinary inputs are allowed")
        }
        slot -> inpValue
      }

      work(SlotValueMap(inputVals))
    } finally {
      inputLock.readLock().unlock()
    }
  }
}
