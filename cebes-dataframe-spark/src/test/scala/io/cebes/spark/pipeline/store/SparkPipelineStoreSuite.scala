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

import io.cebes.common.HasId
import io.cebes.pipeline.PipelineStore
import io.cebes.pipeline.models.Pipeline
import io.cebes.spark.CebesSparkTestInjector
import io.cebes.spark.helpers.{TestDataHelper, TestPropertyHelper}
import io.cebes.spark.pipeline.etl.{Join, Sample}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SparkPipelineStoreSuite extends FunSuite with BeforeAndAfterAll with TestPropertyHelper with TestDataHelper {

  private def samplePipeline = {
    val stage1 = Join()
    stage1.setName("stage1").set(stage1.joinType, "outer")
    val stage2 = Sample()
    stage2.setName("stage2")
      .set(stage2.withReplacement, false)
      .set(stage2.fraction, 0.2)
      .set(stage2.seed, 169L)

    Pipeline(HasId.randomId, Map(stage1.getName -> stage1, stage2.getName -> stage2))
  }

  test("add and get") {
    val plStore = CebesSparkTestInjector.instance[PipelineStore]
    val pl = samplePipeline
    val plId = pl.id
    plStore.add(pl)

    val pl2 = plStore(plId)
    assert(pl2.eq(pl))

    val ex = intercept[IllegalArgumentException](plStore(UUID.randomUUID()))
    assert(ex.getMessage.startsWith("Object ID not found"))
  }

  test("persist and unpersist") {
    val plStore = CebesSparkTestInjector.instance[PipelineStore]
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
