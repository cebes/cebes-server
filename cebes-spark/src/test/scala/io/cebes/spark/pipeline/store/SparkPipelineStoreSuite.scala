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
 *
 * Created by phvu on 17/12/2016.
 */

package io.cebes.spark.pipeline.store

import java.util.UUID

import com.google.inject.TypeLiteral
import io.cebes.df.functions
import io.cebes.store.CachedStore
import io.cebes.pipeline.json._
import io.cebes.pipeline.models.Pipeline
import io.cebes.spark.CebesSparkTestInjector
import io.cebes.spark.helpers.{ImplicitExecutor, TestDataHelper, TestPipelineHelper, TestPropertyHelper}
import io.cebes.spark.pipeline.etl.{Join, Sample}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SparkPipelineStoreSuite extends FunSuite with BeforeAndAfterAll
  with TestPropertyHelper with TestDataHelper with TestPipelineHelper with ImplicitExecutor {

  private def samplePipeline = {
    val pipelineProto = PipelineDef(None, Array(
      StageDef("stage1", "Join", Map(
        "joinType" -> ValueDef("outer"),
        "joinExprs" -> ColumnDef(functions.col("col1").equalTo(functions.col("col2"))))),
      StageDef("stage2", "Sample", Map(
        "withReplacement" -> ValueDef(false),
        "fraction" -> ValueDef(0.2),
        "seed" -> ValueDef(169L),
        "inputDf" -> StageOutputDef("stage1", "outputDf")))))

    val ppl = pipelineFactory.create(pipelineProto)
    assert(ppl.stages.size === 2)

    assert(ppl.stages("stage1").isInstanceOf[Join])
    val joinStage = ppl.stages("stage1").asInstanceOf[Join]
    assert(joinStage.input(joinStage.joinType).get === "outer")

    assert(ppl.stages("stage2").isInstanceOf[Sample])
    val sampleStage = ppl.stages("stage2").asInstanceOf[Sample]
    assert(sampleStage.input(sampleStage.fraction).get === 0.2)
    assert(sampleStage.input(sampleStage.seed).get === 169L)
    assert(!sampleStage.input(sampleStage.withReplacement).get)

    ppl
  }

  test("add and get") {
    val plStore = CebesSparkTestInjector.instance(new TypeLiteral[CachedStore[Pipeline]](){})
    val pl = samplePipeline
    val plId = pl.id
    plStore.add(pl)

    val pl2 = plStore(plId)
    assert(pl2.eq(pl))

    val ex = intercept[IllegalArgumentException](plStore(UUID.randomUUID()))
    assert(ex.getMessage.startsWith("Object ID not found"))
  }

  test("persist and unpersist") {
    val plStore = CebesSparkTestInjector.instance(new TypeLiteral[CachedStore[Pipeline]](){})
    val pl = samplePipeline
    val plId = pl.id

    // persist, without add to the cache
    plStore.persist(pl)

    // but can still get it
    // this is a test only. Don't do this in production.
    val pl2 = plStore(plId)
    assert(pl2.id === plId)
    // df2 is a different instance from df, although they have the same id
    assert(pl2.ne(pl))

    // unpersist
    plStore.unpersist(plId)

    // cannot get it
    val pl3 = plStore(plId)
    assert(pl3.id === plId)
    assert(pl3.ne(pl))
    assert(pl3.eq(pl2))
  }
}
