/* Copyright 2017 The Cebes Authors. All Rights Reserved.
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
package io.cebes.spark.pipeline.ml.tree

import io.cebes.pipeline.ml.{HasFeaturesCol, HasLabelCol, HasPredictionCol, HasSeed}
import io.cebes.pipeline.models.{InputSlot, RequiredSlotValidator, SlotValidators}

/**
  * Common trait containing input slots for Decision tree regressor and classifier
  */
trait DecisionTreeInputs extends HasFeaturesCol with HasLabelCol with HasPredictionCol with HasSeed {

  val checkpointInterval: InputSlot[Int] = inputSlot[Int]("checkpointInterval",
    "Param for set checkpoint interval (>= 1) or disable checkpoint (-1)", Some(10),
    RequiredSlotValidator[Int]((interval: Int) => interval == -1 || interval >= 1, ""))

  val maxBins: InputSlot[Int] = inputSlot[Int]("maxBins",
    "Max number of bins for discretizing continuous features.  Must be >=2 and >= number of " +
      "categories for any categorical feature.", Some(2), SlotValidators.greaterOrEqual(2))

  val maxDepth: InputSlot[Int] = inputSlot[Int]("maxDepth",
    "Maximum depth of the tree. (>= 0) E.g., depth 0 means 1 leaf node; depth 1 means 1 internal " +
      "node + 2 leaf nodes.", Some(0), SlotValidators.greaterOrEqual(0))

  val minInfoGain: InputSlot[Double] = inputSlot[Double]("minInfoGain",
    "Minimum information gain for a split to be considered at a tree node.",
    Some(0.0), SlotValidators.greaterOrEqual(0.0))

  val minInstancesPerNode: InputSlot[Int] = inputSlot[Int]("minInstancesPerNode",
    "Minimum number of instances each child must have after split. " +
      "If a split causes the left or right child to have fewer than minInstancesPerNode, " +
      "the split will be discarded as invalid. Should be >= 1.", Some(1), SlotValidators.greaterOrEqual(1))

  val maxMemoryInMB: InputSlot[Int] = inputSlot[Int]("maxMemoryInMB",
    "Maximum memory in MB allocated to histogram aggregation.", Some(256),
    SlotValidators.greaterOrEqual(0))

  final val cacheNodeIds: InputSlot[Boolean] = inputSlot[Boolean]("cacheNodeIds",
    "If false, the algorithm will pass trees to executors to match instances with nodes. If true, the" +
      " algorithm will cache node IDs for each instance. Caching can speed up training of deeper trees.",
    Some(false))
}
