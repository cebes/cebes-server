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

import io.cebes.pipeline.ModelService
import io.cebes.pipeline.json.ModelRunDef
import io.cebes.pipeline.ml.Model

class DummyModelService extends ModelService {
  /**
    * Store the given [[Model]] in the cache
    *
    * @return the given model object
    */
  override def cache(model: Model) = ???

  /**
    * Run the given model on the input [[io.cebes.df.Dataframe]]
    *
    * @param runRequest the request.
    *                   See documentation of [[ModelRunDef]] for more information.
    * @return The resulting [[io.cebes.df.Dataframe]], after running transform on the input data
    */
  override def run(runRequest: ModelRunDef) = ???

  override def cachedStore = ???

  override def tagStore = ???
}
