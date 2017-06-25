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
package io.cebes.pipeline

import io.cebes.pipeline.json._
import io.cebes.pipeline.ml.Model
import io.cebes.tag.TagService

trait ModelService extends TagService[Model] {

  /**
    * Store the given [[Model]] in the cache
    * @return the given model object
    */
  def cache(model: Model): Model

  /**
    * Run the given model on the input [[io.cebes.df.Dataframe]]
    *
    * @param runRequest the request.
    *                   See documentation of [[ModelRunDef]] for more information.
    * @return The resulting [[io.cebes.df.Dataframe]], after running transform on the input data
    */
  def run(runRequest: ModelRunDef): DataframeMessageDef
}
