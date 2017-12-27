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
 * Created by phvu on 29/09/16.
 */

package io.cebes.spark.df

import io.cebes.spark.helpers.{TestDataHelper, TestPropertyHelper}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import spray.json.{JsArray, JsObject}

class SparkDataframeServiceSuite extends FunSuite with BeforeAndAfterAll
  with TestPropertyHelper with TestDataHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    createOrReplaceCylinderBands()
  }

  test("Simple SQL") {
    val df = sparkDataframeService.sql(s"SELECT * FROM $cylinderBandsTableName")
    assert(df.numCols === 40)
    assert(df.numRows === 540)
    val sample = df.take(10)
    assert(sample.numCols === df.numCols)
  }

  test("JSON serialization") {
    val df = getCylinderBands.limit(10)
    val js = sparkDataframeService.serialize(df)
    val jsObj = js.asJsObject
    assert(jsObj.fields.contains("schema"))
    assert(jsObj.fields.contains("data"))
    assert(jsObj.fields("data").asInstanceOf[JsArray].elements.length === 10)

    // deserialize the same JSON object
    val df2 = sparkDataframeService.deserialize(jsObj)
    assert(df2.numRows === df.numRows)
    assert(df.columns.zip(df2.columns).forall { case (c1, c2) => c1 === c2 })
    assert(df.schema === df2.schema)

    // deserialize only data, skip the schema
    val js2 = JsObject(Map("data" -> jsObj.fields("data")))
    val df3 = sparkDataframeService.deserialize(js2)
    assert(df3.numRows === df.numRows)
    assert(df3.columns.forall(df.columns.contains))

    // empty dataframe: zero rows
    val df0 = getCylinderBands.limit(0)
    assert(df0.numRows === 0)
    assert(df0.columns === df.columns)
    val js3 = sparkDataframeService.serialize(df0)
    val df01 = sparkDataframeService.deserialize(JsObject(Map("data" -> js3.asJsObject.fields("data"))))
    assert(df01.numRows === df0.numRows)
    assert(df01.columns.isEmpty)
  }
}
