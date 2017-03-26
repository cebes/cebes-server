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
package io.cebes.spark.pipeline.ml.regression

import io.cebes.spark.helpers.{ImplicitExecutor, TestDataHelper, TestPipelineHelper}
import org.scalatest.FunSuite

class LinearRegressionSuite extends FunSuite with ImplicitExecutor with TestDataHelper with TestPipelineHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    createOrReplaceCylinderBands()
  }

  /*
  test("Linear regression simple") {
    val lr = CebesSparkTestInjector.instance[LinearRegression]
    assert(lr.getName === "linearregression")
    lr.input(lr.featuresCol, "customer")
    lr.input(lr.labelCol, "band_type")
    lr.input(lr.predictionCol, "band_type_predict")
    lr.input(lr.data, getCylinderBands.limit(200))

    //TODO: changing some inputs (e.g. featureCol, labelCol, etc...)
    // should actually clear the stateful output (i.e. model)
    // while changing `data` shouldn't change the stateful output
    // There should be a way to specify which input will effect stateful outputs

    val lrModel = lr.getModel()
    assert(lrModel.isInstanceOf[LinearRegressionModel])

    val dfPredict = Await.result(lr.output(lr.predict).getFuture, Duration.Inf)
    print(dfPredict.schema)
  }
  */
}
