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
package io.cebes.pipeline.inject

import io.cebes.pipeline.factory.ModelFactory
import io.cebes.pipeline.json.ModelDef
import io.cebes.pipeline.ml.Model

class DummyModelFactory extends ModelFactory {
  /**
    * Create a model from the given [[ModelDef]]
    *
    * @param modelDef   the [[ModelDef]] object
    * @param storageDir the storage directory to serialize auxiliary data, if needed.
    *                   If not specified, use system-wide default value
    * @return a [[Model]] object
    */
  override def create(modelDef: ModelDef, storageDir: Option[String]) = ???

  /**
    * Serialize a [[Model]] into a [[ModelDef]]
    *
    * @param model      the model instance to be serialized
    * @param storageDir the storage directory to read auxiliary data from, if needed.
    *                   If not specified, use system-wide default value
    */
  override def save(model: Model, storageDir: Option[String]) = ???
}
