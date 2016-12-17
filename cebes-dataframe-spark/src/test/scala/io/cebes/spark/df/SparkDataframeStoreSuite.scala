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

package io.cebes.spark.df

import java.util.UUID

import io.cebes.df.DataframeStore
import io.cebes.spark.CebesSparkTestInjector
import io.cebes.spark.helpers.{CebesBaseSuite, TestDataHelper, TestPropertyHelper}

class SparkDataframeStoreSuite extends CebesBaseSuite
  with TestPropertyHelper with TestDataHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    createOrReplaceCylinderBands()
  }

  test("simple operations") {
    val dfStore = CebesSparkTestInjector.instance[DataframeStore]
    val df = getCylinderBands
    val dfId = df.id
    dfStore.add(df)

    val df2 = dfStore(dfId)
    assert(df2.eq(df))

    val ex = intercept[IllegalArgumentException](dfStore(UUID.randomUUID()))
    assert(ex.getMessage.startsWith("Dataframe ID not found"))
  }
}
