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
package io.cebes.pipeline.exporter

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import com.google.inject.Inject
import io.cebes.pipeline.factory.ModelFactory
import io.cebes.pipeline.json.ModelDef
import io.cebes.pipeline.ml.Model
import spray.json._

class ModelExporter @Inject()(private val modelFactory: ModelFactory) {

  /**
    * Export the given model into the given storage directory
    * The [[ModelDef]] definition will be written into the path specified in `modelDefFilePath`
    */
  def export(model: Model, storageDir: String, modelDefFilePath: String)
            (implicit jsonWriter: JsonWriter[ModelDef]): String = {
    val modelDef = modelFactory.save(model, Some(storageDir))
    val modelDefUpdated = modelDef.copy(metaData = modelDef.metaData
      ++ Map(ModelExporter.METADATA_STORAGE_DIR -> storageDir))

    Files.write(Paths.get(modelDefFilePath), modelDefUpdated.toJson.compactPrint.getBytes(StandardCharsets.UTF_8))
    modelDefFilePath
  }

  /**
    * Import a [[ModelDef]] at the given location, which was exported previously with [[export()]]
    * Returns the deserialized [[Model]] object
    */
  def imports(modelDefFilePath: String)(implicit jsonReader: JsonReader[ModelDef]): Model = {
    val modelDef = Files.readAllLines(Paths.get(modelDefFilePath), StandardCharsets.UTF_8)
      .toArray.mkString("\n").parseJson.convertTo[ModelDef]
    val storageDir = modelDef.metaData.get(ModelExporter.METADATA_STORAGE_DIR) match {
      case None =>
        throw new IllegalArgumentException(s"Could not find storageDir for model ${modelDef.id}")
      case Some(p) => p
    }
    modelFactory.create(modelDef, Some(storageDir))
  }
}

object ModelExporter {

  private val METADATA_STORAGE_DIR = s"${getClass.getName}/storageDir"
}