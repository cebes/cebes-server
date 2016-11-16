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
 * Created by phvu on 14/11/2016.
 */

package io.cebes.df.schema

import io.cebes.df.types.VariableTypes
import io.cebes.df.types.storage.{FloatType, IntegerType, StringType}
import org.scalatest.FunSuite

class SchemaSuite extends FunSuite {

  test("Schema construction") {
    val sc = Schema()
    assert(sc.length === 0)

    val sc2 = Schema().add("a", IntegerType).add(new SchemaField("b", FloatType))
    assert(sc2.length === 2)
    assert(sc2.fieldNames === Array("a", "b"))

    val sc3 = Schema().add("a", IntegerType, VariableTypes.NOMINAL)
    assert(sc3.remove("b").length === 1)
    assert(sc3.remove("A").length === 0)
    assert(sc3.length === 1)

    intercept[IllegalArgumentException] {
      Schema().add("a", IntegerType, VariableTypes.TEXT)
    }
  }

  test("withField") {
    val sc = Schema().withField("a", IntegerType).withField("b", StringType)
    assert(sc.length === 2)
    assert(sc.fieldNames === Array("a", "b"))

    // add
    val sc2 = sc.withField("c", FloatType)
    assert(sc2.length === 3)
    assert(sc2.fieldNames === Array("a", "b", "c"))

    val sc3 = sc.withField("c", FloatType, VariableTypes.CONTINUOUS)
    assert(sc3.length === 3)
    assert(sc3.fieldNames === Array("a", "b", "c"))

    // replace
    val sc4 = sc.withField("A", StringType)
    assert(sc4.length === 2)
    assert(sc4.fieldNames === Array("A", "b"))
    assert(sc4("a").storageType === StringType)
  }

  test("withFieldRenamed") {
    val sc = Schema().withField("a", IntegerType).withField("b", StringType)
    assert(sc.length === 2)
    assert(sc.fieldNames === Array("a", "b"))

    // success
    val sc2 = sc.withFieldRenamed("A", "aa")
    assert(sc2.ne(sc))
    assert(sc2.length === 2)
    assert(sc2.fieldNames === Array("aa", "b"))
    assert(sc2("aa").storageType === IntegerType)
    assert(sc2("b").storageType === StringType)

    // no-op
    val sc3 = sc.withFieldRenamed("AA", "aa")
    assert(sc3.eq(sc))
  }
}
