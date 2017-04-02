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

import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.LazyLogging
import io.cebes.df.Dataframe
import io.cebes.pipeline.models._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

/**
  * A special subclass of [[Stage]] that estimates a Machine Learning model.
  * Has an input slot of type [[Dataframe]] for the training set,
  * an output slot of type [[Model]] for the resulting model,
  * and is non-deterministic by default.
  * Subclasses can add more output slot to output additional information related to the training process.
  */
trait Estimator extends Stage with LazyLogging {

  val inputDf: InputSlot[Dataframe] = inputSlot[Dataframe]("inputDf", "The training dataset", None)
  val model: OutputSlot[Model] = outputSlot[Model]("model",
    "The output model of this estimator", None, stateful = true)

  val outputDf: OutputSlot[Dataframe] = outputSlot[Dataframe]("outputDf",
    "The result dataframe transformed by the model", None)

  /**
    * Train (if needed) and return the model.
    * Note that other outputs of the [[Estimator]] (if there is any) can still
    * be accessed normally after this call, using the [[output()]] function.
    *
    * @param atMost the maximum time allowed for training. If it takes longer to train,
    *               the function will fail.
    * @return the trained model
    */
  def getModel(atMost: Duration = Duration(2, TimeUnit.MINUTES))(implicit ec: ExecutionContext): Model = {
    output(model).getResult(atMost)(ec)
  }
}
