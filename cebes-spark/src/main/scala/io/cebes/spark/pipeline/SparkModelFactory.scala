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
package io.cebes.spark.pipeline

import com.google.inject.Inject
import io.cebes.pipeline.factory.ModelFactory
import io.cebes.pipeline.json.ModelDef
import io.cebes.pipeline.ml.Model
import io.cebes.pipeline.models.PipelineMessageSerializer
import io.cebes.prop.{Prop, Property}
import io.cebes.spark.df.SparkDataframeFactory
import io.cebes.spark.pipeline.ml.traits.SparkModel

/**
  * Implementation of [[io.cebes.pipeline.factory.ModelFactory]] on Spark
  */
class SparkModelFactory @Inject()(private val msgSerializer: PipelineMessageSerializer,
                                  private val dfFactory: SparkDataframeFactory,
                                  @Prop(Property.MODEL_STORAGE_DIR) private val modelStorageDir: String)
  extends ModelFactory {

  override def create(modelDef: ModelDef): Model = {
    val cls = Class.forName(modelDef.modelClass)
    require(cls.getInterfaces.contains(classOf[SparkModel]), s"${getClass.getName} only support models " +
      s"that implement ${classOf[SparkModel].getName}. Got ${modelDef.modelClass}.")
    SparkModel.fromModelDef(modelDef, msgSerializer, dfFactory, modelStorageDir)
  }

  override def save(model: Model): ModelDef = {
    val cls = model.getClass
    require(cls.getInterfaces.contains(classOf[SparkModel]), s"${getClass.getName} only support models " +
      s"that implement ${classOf[SparkModel].getName}. Got ${cls.getName}.")
    model.asInstanceOf[SparkModel].toModelDef(msgSerializer, modelStorageDir)
  }
}
