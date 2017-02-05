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

import com.typesafe.scalalogging.LazyLogging
import io.cebes.df.Dataframe
import io.cebes.pipeline.models._

/**
  * A special subclass of [[Stage]] that estimates a Machine Learning model.
  * Has an input slot of type [[Dataframe]] for the training set,
  * an output slot of type [[Model]] for the resulting model,
  * and is non-deterministic by default.
  * Subclasses can add more output slot to output additional information related to the training process.
  */
trait Estimator extends Stage with LazyLogging {

  val data: InputSlot[Dataframe] = inputSlot[Dataframe]("data", "The training dataset", None)
  val model: OutputSlot[Model] = outputSlot[Model]("model", "The output model of this estimator", None)

  override def nonDeterministic: Boolean = true

  /**
    * Helper to copy all the ordinary inputs from this estimator to `dest`
    * Normally used to copy the input from the estimator to the output model
    * Return the destination object
    */
  final protected def copyOrdinaryInputs(dest: Inputs): Inputs = {
    _inputs.foreach { slot =>
      input(slot) match {
        case ordinaryInput: OrdinaryInput[_] =>
          if (dest.hasInput(slot.name)) {
            dest.input(dest.getInput(slot.name), ordinaryInput.copy())
          } else {
            logger.warn(s"$toString: destination class ${dest.toString} does not have input name ${slot.name}")
          }
        case _ =>
          // ignore other kinds of input
      }
    }
    dest
  }
}
