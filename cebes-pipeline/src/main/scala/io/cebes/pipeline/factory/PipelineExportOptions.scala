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
package io.cebes.pipeline.factory

/**
  * Options controlling how [[PipelineFactory]] works
  *
  * Normally for usual checkpointing (in cebes-http-server for example), both includeModels and includeSchemas
  * are false. They are only true when exporting the [[io.cebes.pipeline.models.Pipeline]]s for serving.
  *
  * @param includeModels     Whether to include the [[io.cebes.pipeline.json.ModelDef]] definition into the
  *                          [[io.cebes.pipeline.json.StageDef]] definitions.
  * @param modelStorageDir   the directory to store the models. If None, system-wide default values will be used.
  *                          Only considered when `includeModels` is `true`.
  * @param includeSchemas    whether to include the schemas of [[io.cebes.df.Dataframe]]
  *                          in the [[io.cebes.pipeline.json.StageDef]] definitions.
  * @param includeDataframes whether to include the [[io.cebes.df.Dataframe]] if they are values to the stage slots.
  *                          If yes, only the Dataframe ID will be recorded, making the exported pipeline not portable
  *                          between servers.
  */
case class PipelineExportOptions(includeModels: Boolean = false,
                                 modelStorageDir: Option[String] = None,
                                 includeSchemas: Boolean = false,
                                 includeDataframes: Boolean = false)
