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

package io.cebes.spark.df.store

import java.util.UUID

import io.cebes.df.store.DataframeStore
import io.cebes.spark.CebesSparkTestInjector
import io.cebes.spark.helpers.{TestDataHelper, TestPropertyHelper}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SparkDataframeStoreSuite extends FunSuite with BeforeAndAfterAll
  with TestPropertyHelper with TestDataHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    createOrReplaceCylinderBands()
  }

  test("add and get") {
    val dfStore = CebesSparkTestInjector.instance[DataframeStore]
    val df = getCylinderBands
    val dfId = df.id
    dfStore.add(df)

    val df2 = dfStore(dfId)
    assert(df2.eq(df))

    val ex = intercept[IllegalArgumentException](dfStore(UUID.randomUUID()))
    assert(ex.getMessage.startsWith("Object ID not found"))
  }

  test("persist and unpersist") {
    val dfStore = CebesSparkTestInjector.instance[DataframeStore]
    val df = getCylinderBands.limit(100)
    val dfId = df.id

    // persist, without add to the cache
    dfStore.persist(df)

    // but can still get it
    // this is a test only. Don't do this in production.
    val df2 = dfStore(dfId)
    assert(df2.id === dfId)
    // df2 is a different instance from df, although they have the same id
    assert(!df2.eq(df))

    // unpersist
    dfStore.unpersist(dfId)

    // but can still get it, because it is in the cache
    val df3 = dfStore(dfId)
    assert(df3.id === dfId)
    assert(!df3.eq(df))
    assert(df3.eq(df2))
  }
}
