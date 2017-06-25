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
package io.cebes.spark.pipeline.features

import java.util.UUID

import com.google.inject.Inject
import io.cebes.common.HasId
import io.cebes.pipeline.models.{InputSlot, SlotValidators, SlotValueMap}
import io.cebes.spark.df.SparkDataframeFactory
import io.cebes.spark.pipeline.ml.traits.{SparkEstimator, SparkModel}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.{VectorIndexer => SparkVectorIndexer}
import org.apache.spark.ml.util.MLWritable

trait VectorIndexerInputs extends HasInputCol with HasOutputCol {

  val maxCategories: InputSlot[Int] = inputSlot[Int]("maxCategories",
    "Threshold for the number of values a categorical feature can take", Some(20),
    SlotValidators.greaterOrEqual(2))
}

/**
  * Light wrapper of Spark's VectorIndexer
  *
  * Class for indexing categorical feature columns in a dataset of `Vector`.
  *
  * This has 2 usage modes:
  *  - Automatically identify categorical features (default behavior)
  *     - This helps process a dataset of unknown vectors into a dataset with some continuous
  * features and some categorical features. The choice between continuous and categorical
  * is based upon a maxCategories parameter.
  *     - Set maxCategories to the maximum number of categorical any categorical feature should have.
  *     - E.g.: Feature 0 has unique values {-1.0, 0.0}, and feature 1 values {1.0, 3.0, 5.0}.
  * If maxCategories = 2, then feature 0 will be declared categorical and use indices {0, 1},
  * and feature 1 will be declared continuous.
  *  - Index all features, if all features are categorical
  *     - If maxCategories is set to be very large, then this will build an index of unique
  * values for all features.
  *     - Warning: This can cause problems if features are continuous since this will collect ALL
  * unique values to the driver.
  *     - E.g.: Feature 0 has unique values {-1.0, 0.0}, and feature 1 values {1.0, 3.0, 5.0}.
  * If maxCategories is greater than or equal to 3, then both features will be declared
  * categorical.
  *
  * This returns a model which can transform categorical features to use 0-based indices.
  *
  * Index stability:
  *  - This is not guaranteed to choose the same category index across multiple runs.
  *  - If a categorical feature includes value 0, then this is guaranteed to map value 0 to index 0.
  * This maintains vector sparsity.
  *  - More stability may be added in the future.
  */
case class VectorIndexer @Inject()(dfFactory: SparkDataframeFactory)
  extends SparkEstimator with VectorIndexerInputs {

  override protected def estimate(inputs: SlotValueMap): SparkModel = {
    val indexer = new SparkVectorIndexer()
      .setInputCol(inputs(inputCol))
      .setOutputCol(inputs(outputCol))
      .setMaxCategories(inputs(maxCategories))

    val df = getSparkDataframe(inputs(inputDf)).sparkDf
    VectorIndexerModel(HasId.randomId, indexer.fit(df), dfFactory)
  }
}

case class VectorIndexerModel(id: UUID, sparkTransformer: Transformer with MLWritable,
                              dfFactory: SparkDataframeFactory)
  extends SparkModel with VectorIndexerInputs
