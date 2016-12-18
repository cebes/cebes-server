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

import io.cebes.df.CebesCoreJsonProtocol._
import io.cebes.df.types.storage._
import io.cebes.df.types.{StorageTypes, VariableTypes}
import org.scalatest.FunSuite
import spray.json._

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

  test("serialize and deserialize") {
    val sc = Schema().withField("string_col", StringType).withField("binary_col", BinaryType)
      .withField("date_col", DateType).withField("timestamp_col", TimestampType)
      .withField("calendar_interval_col", CalendarIntervalType)
      .withField("boolean_col", BooleanType).withField("byte_col", ByteType)
      .withField("short_col", ShortType).withField("integer_col", IntegerType)
      .withField("long_col", LongType, VariableTypes.ORDINAL)
      .withField("float_col", FloatType).withField("double_col", DoubleType)
      .withField("array_float_col", StorageTypes.arrayType(FloatType))
      .withField("array_double_col", StorageTypes.arrayType(DoubleType))
      .withField("array_int_col", StorageTypes.arrayType(IntegerType))
      .withField("map_int_string_col", StorageTypes.mapType(IntegerType, StringType))
      .withField("struct_col", StorageTypes.structType(
        StorageTypes.structField("field1", StringType, Metadata.empty),
        StorageTypes.structField("field2", StringType,
          new MetadataBuilder().putNull("null_meta").build()),
        StorageTypes.structField("field2", StringType,
          new MetadataBuilder().putNull("null_meta").putDoubleArray("double_arr_meta", Array(1.0, 2.0)).build())
      ))

    val serialized = sc.toJson.compactPrint
    val sc2 = serialized.parseJson.convertTo[Schema]
    assert(sc2.length === sc.length)
    // non-default variable type
    assert(sc2.get("long_col").get.variableType === VariableTypes.ORDINAL)
    assert(sc2.get("array_float_col").get.storageType === StorageTypes.arrayType(FloatType))
    assert(sc2.equals(sc))

    // deserialize from non-sense string
    intercept[DeserializationException] {
      """{"non-sense": 100}""".parseJson.convertTo[Schema]
    }
  }
}
