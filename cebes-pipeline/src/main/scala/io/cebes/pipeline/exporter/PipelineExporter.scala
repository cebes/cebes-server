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
import java.nio.file.{Files, Path, Paths}

import com.google.inject.Inject
import io.cebes.pipeline.json.{ModelDef, PipelineDef, PipelineExportDef}
import io.cebes.pipeline.models.{Pipeline, Stage}
import spray.json.{JsonReader, JsonWriter, _}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/**
  * Provides functions for exporting and importing [[Pipeline]]s
  */
class PipelineExporter @Inject()(private val stageExporter: StageExporter) {
  /**
    * Export the given [[Pipeline]] into downloadable format
    *
    * @param ppl        the pipeline object to be exported
    * @param storageDir the local directory contains the serialized pipeline.
    * @return the path to the directory containing the exported pipeline. Typically
    *         a sub-directory of `storageDir`
    */
  def export(ppl: Pipeline, storageDir: String)
            (implicit jsPplExp: JsonWriter[PipelineExportDef],
             jsModelDef: JsonWriter[ModelDef],
             ec: ExecutionContext): Future[String] = {

    def checkExists(idx: Int): Path = {
      val p = Paths.get(storageDir, if (idx < 0) ppl.id.toString else s"${ppl.id.toString}_$idx")
      if (!Files.exists(p)) {
        p
      } else {
        checkExists(idx + 1)
      }
    }

    val destDir = Files.createDirectories(checkExists(-1)).toString

    val futureStageDefs = ppl.stages.values.map { stage =>
      stageExporter.export(stage, destDir)
    }

    Future.sequence(futureStageDefs).map { stageDefs =>
      val pplExp = PipelineExportDef(PipelineExporter.VERSION, PipelineDef(Some(ppl.id), stageDefs.toArray))

      Files.write(Paths.get(destDir, PipelineExporter.ENTRY_FILE_NAME),
        pplExp.toJson.compactPrint.getBytes(StandardCharsets.UTF_8))
      destDir
    }
  }

  /**
    * Import the exported pipeline at the given `storageDir` into a [[Pipeline]] object
    *
    * @return imported [[Pipeline]] object
    */
  def imports(storageDir: String)
             (implicit jsonReader: JsonReader[PipelineExportDef],
              jsModelDef: JsonReader[ModelDef],
              ec: ExecutionContext): Pipeline = {

    val entryFile = Paths.get(storageDir, PipelineExporter.ENTRY_FILE_NAME)
    require(Files.exists(entryFile), s"A file named ${PipelineExporter.ENTRY_FILE_NAME} is required under $storageDir")

    val pplExp = Files.readAllLines(entryFile, StandardCharsets.UTF_8)
      .toArray.mkString("\n").parseJson.convertTo[PipelineExportDef]
    require(pplExp.version == PipelineExporter.VERSION, s"Incompatible versions: " +
      s"input pipeline of version ${pplExp.version} being read for version ${PipelineExporter.VERSION}")

    val pplDef = pplExp.pipeline
    val stageMap = mutable.Map.empty[String, Stage]
    pplDef.stages.map { s =>
      val stage = stageExporter.imports(s, storageDir)
      require(!stageMap.contains(stage.getName), s"Duplicated stage name: ${stage.getName}")
      stageMap.put(stage.getName, stage)
    }
    Pipeline(pplDef.id.get, stageMap.toMap, pplDef)
  }
}

object PipelineExporter {
  private val VERSION = "1"
  private val ENTRY_FILE_NAME = "pipeline.json"
}
