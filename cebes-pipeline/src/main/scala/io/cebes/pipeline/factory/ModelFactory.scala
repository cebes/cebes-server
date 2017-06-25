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
package io.cebes.pipeline.factory

import io.cebes.pipeline.json.ModelDef
import io.cebes.pipeline.ml.Model

/**
  * Generic trait for a factory of [[Model]]s
  */
trait ModelFactory {

  /**
    * Create a model from the given [[ModelDef]]
    */
  def create(modelDef: ModelDef): Model

  /**
    * Serialize a [[Model]] into a [[ModelDef]]
    * @param model the model instance to be serialized
    */
  def save(model: Model): ModelDef
}
