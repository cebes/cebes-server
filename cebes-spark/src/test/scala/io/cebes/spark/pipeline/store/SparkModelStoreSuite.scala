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
import io.cebes.pipeline.ml.Model
import io.cebes.spark.helpers.{ImplicitExecutor, TestDataHelper, TestPipelineHelper, TestPropertyHelper}
import io.cebes.spark.pipeline.features.VectorAssembler
import io.cebes.spark.pipeline.ml.regression.LinearRegression
import io.cebes.store.CachedStore
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SparkModelStoreSuite extends FunSuite with BeforeAndAfterAll
  with TestPropertyHelper with TestDataHelper with TestPipelineHelper with ImplicitExecutor {

  override def beforeAll(): Unit = {
    super.beforeAll()
    createOrReplaceCylinderBands()
  }

  private def sampleModel = {
    val df = getCylinderBands.limit(200).na.drop()
    assert(df.numRows > 1)

    val assembler = getInstance[VectorAssembler]
    assembler.input(assembler.inputCols, Array("viscosity", "proof_cut"))
      .input(assembler.outputCol, "features")
      .input(assembler.inputDf, df)

    val lr = getInstance[LinearRegression]
    lr.input(lr.featuresCol, "features")
      .input(lr.labelCol, "caliper")
      .input(lr.predictionCol, "caliper_predict")
      .input(lr.inputDf, assembler.output(assembler.outputDf))

    lr.getModel(TEST_WAIT_TIME)
  }

  test("add and get") {
    val modelStore = getInstance(new TypeLiteral[CachedStore[Model]]() {})
    val model = sampleModel
    val plId = model.id
    modelStore.add(model)

    val model2 = modelStore(plId)
    assert(model2.eq(model))

    val ex = intercept[IllegalArgumentException](modelStore(UUID.randomUUID()))
    assert(ex.getMessage.startsWith("Object ID not found"))
  }

  test("persist and unpersist") {
    val modelStore = getInstance(new TypeLiteral[CachedStore[Model]]() {})
    val model = sampleModel
    val modelId = model.id

    // persist, without add to the cache
    modelStore.persist(model)

    // but can still get it
    // this is a test only. Don't do this in production.
    val model2 = modelStore(modelId)
    assert(model2.id === modelId)
    // df2 is a different instance from df, although they have the same id
    assert(model2.ne(model))

    // unpersist
    modelStore.unpersist(modelId)

    // cannot get it
    val model3 = modelStore(modelId)
    assert(model3.id === modelId)
    assert(model3.ne(model))
    assert(model3.eq(model2))
  }
}
