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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import com.google.inject.Inject
import io.cebes.common.HasId
import io.cebes.pipeline.json.{PipelineDef, PipelineExportDef}
import io.cebes.pipeline.models.{Pipeline, Stage}
import io.cebes.util.FileSystemHelper
import spray.json._

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/**
  * Provides functions for exporting and importing [[Pipeline]]s
  */
class PipelineFactory @Inject()(private val stageFactory: StageFactory) {

  /**
    * Export the given [[Pipeline]] into a [[PipelineExportDef]] message
    *
    * @param ppl     the pipeline object to be exported
    * @param options See [[PipelineExportOptions]]
    * @return
    */
  def export(ppl: Pipeline, options: PipelineExportOptions)
            (implicit ec: ExecutionContext): Future[PipelineExportDef] = {
    val futureStageDefs = ppl.stages.values.map { stage =>
      stageFactory.export(stage, options)
    }

    Future.sequence(futureStageDefs).map { stageDefs =>
      PipelineExportDef(PipelineFactory.VERSION, PipelineDef(Some(ppl.id), stageDefs.toArray))
    }
  }

  /**
    * Export the given [[Pipeline]] into downloadable format
    *
    * @param ppl        the pipeline object to be exported
    * @param storageDir the local directory contains the serialized pipeline.
    * @return the path to the directory containing the exported pipeline. Typically
    *         a sub-directory of `storageDir`
    */
  def export(ppl: Pipeline, storageDir: String)(implicit jsPplExp: JsonWriter[PipelineExportDef],
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
    val modelDir = Files.createDirectory(Paths.get(destDir, PipelineFactory.MODEL_SUB_DIR))
    val options = PipelineExportOptions(includeModels = true, modelStorageDir = Some(modelDir.toString),
      includeSchemas = true)
    export(ppl, options).map { pplExp =>
      Files.write(Paths.get(destDir, PipelineFactory.ENTRY_FILE_NAME),
        pplExp.toJson.compactPrint.getBytes(StandardCharsets.UTF_8))
      destDir
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // pack a pipeline into a zip package
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
    * export the pipeline into a downloadable zip package
    */
  def exportZip(ppl: Pipeline, outputFile: String)(implicit jsPplExp: JsonWriter[PipelineExportDef],
                                                   ec: ExecutionContext): Future[String] = {
    Future(Files.createTempDirectory("cebes-ppl-export")).flatMap { p =>
      export(ppl, p.toString)
    }.map { p =>
      val folder = Paths.get(p)
      val packageFile = Files.createTempFile("cebes-ppl", s"${ppl.id.toString}.zip").toString
      try {
        FileSystemHelper.zipFolder(p, packageFile)
      } finally {
        FileSystemHelper.deleteRecursively(folder.getParent.toFile)
      }
      packageFile
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // imports
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
    * Import the given [[PipelineDef]] into a [[Pipeline]] object
    *
    * @param pplDef          the pipeline definition
    * @param modelStorageDir optional, the directory where all the [[io.cebes.pipeline.ml.Model]] in this
    *                        pipeline were serialized in. If None, use system default value
    *                        (see [[ModelFactory]] for more information.
    */
  def imports(pplDef: PipelineDef, modelStorageDir: Option[String])(implicit ec: ExecutionContext): Pipeline = {
    val pplDefWithId = pplDef.copy(id = pplDef.id.orElse(Some(HasId.randomId)))

    val stageMap = mutable.Map.empty[String, Stage]
    pplDefWithId.stages.map { s =>
      val stage = stageFactory.imports(s, modelStorageDir)
      require(!stageMap.contains(stage.getName), s"Duplicated stage name: ${stage.getName}")
      stageMap.put(stage.getName, stage)
    }
    Pipeline(pplDefWithId.id.get, stageMap.toMap, pplDefWithId)
  }

  /**
    * Import the exported pipeline at the given `storageDir` into a [[Pipeline]] object
    *
    * @return imported [[Pipeline]] object
    */
  def imports(storageDir: String)(implicit jsonReader: JsonReader[PipelineExportDef],
                                  ec: ExecutionContext): Pipeline = {

    val entryFile = Paths.get(storageDir, PipelineFactory.ENTRY_FILE_NAME)
    require(Files.exists(entryFile), s"A file named ${PipelineFactory.ENTRY_FILE_NAME} is required under $storageDir")

    val pplExp = Files.readAllLines(entryFile, StandardCharsets.UTF_8)
      .toArray.mkString("\n").parseJson.convertTo[PipelineExportDef]
    require(pplExp.version == PipelineFactory.VERSION, s"Incompatible versions: " +
      s"input pipeline of version ${pplExp.version} being read for version ${PipelineFactory.VERSION}")

    val modelDir = Paths.get(storageDir, PipelineFactory.MODEL_SUB_DIR)
    if (!Files.exists(modelDir)) {
      Files.createDirectory(modelDir)
    }
    imports(pplExp.pipeline, Some(modelDir.toString))
  }

  ///////////////////////////////////////////////////////////////////////////////////////////
  // import zip
  ///////////////////////////////////////////////////////////////////////////////////////////

  /**
    * Import the given package and return a pipeline object
    */
  def importZip(packageFile: String)(implicit jsonReader: JsonReader[PipelineExportDef],
                                     ec: ExecutionContext): Pipeline = {
    val outDir = Files.createTempDirectory("cebes-ppl-import")
    FileSystemHelper.unzip(packageFile, outDir.toString)
    val ppl = imports(outDir.toString)
    FileSystemHelper.deleteRecursively(outDir.toFile)
    ppl
  }
}

object PipelineFactory {
  private val VERSION = "1"
  private val ENTRY_FILE_NAME = "pipeline.json"
  private val MODEL_SUB_DIR = "models"
}
