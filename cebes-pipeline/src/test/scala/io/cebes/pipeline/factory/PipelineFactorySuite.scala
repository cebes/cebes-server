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

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

import io.cebes.pipeline.inject.PipelineTestInjector
import io.cebes.pipeline.json.PipelineDefaultJsonProtocol._
import io.cebes.pipeline.json.{PipelineDef, StageDef, ValueDef}
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

class PipelineFactorySuite extends FunSuite {

  test("export and import") {
    val exporter = PipelineTestInjector.instance[PipelineFactory]

    val pipelineDef1 = PipelineDef(None, Array(
      StageDef("stage1", "StageTwoInputs", Map(
        "m" -> ValueDef("my input value"),
        "valIn" -> ValueDef(Array(10))))))

    val ppl1 = exporter.imports(pipelineDef1, None)
    val testDir = Files.createTempDirectory("test-ppl-export-")
    val destDir = Await.result(exporter.export(ppl1, testDir.toString), Duration(30, TimeUnit.SECONDS))
    assert(Files.isDirectory(Paths.get(destDir)))

    val ppl2 = exporter.imports(destDir)
    assert(ppl2.id === ppl1.id)
    assert(ppl2.stages.size === 1)
    assert(ppl2.stages("stage1").getName === "stage1")

    def deleteRecursively(file: File): Unit = {
      if (file.isDirectory)
        file.listFiles.foreach(deleteRecursively)
      if (file.exists && !file.delete)
        throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
    }

    deleteRecursively(testDir.toFile)
  }
}
