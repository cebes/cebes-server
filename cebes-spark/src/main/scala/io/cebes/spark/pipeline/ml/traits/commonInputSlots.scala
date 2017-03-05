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
package io.cebes.spark.pipeline.ml.traits

import io.cebes.pipeline.models.{HasInputSlots, InputSlot}

trait HasFeaturesCol extends HasInputSlots {

  val featuresCol: InputSlot[String] = inputSlot[String]("featuresCol",
    "Name of the features column", Some("features"))
}

trait HasLabelCol extends HasInputSlots {

  val labelCol: InputSlot[String] = inputSlot[String]("labelCol",
    "Name of the label column", Some("label"))
}

trait HasPredictionCol extends HasInputSlots {

  val predictionCol: InputSlot[String] = inputSlot[String]("predictionCol",
    "Name of the prediction column", Some("prediction"))
}
