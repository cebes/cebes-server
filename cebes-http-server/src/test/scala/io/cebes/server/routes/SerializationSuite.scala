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
 * Created by phvu on 18/12/2016.
 */

package io.cebes.server.routes

import io.cebes.df.sample.DataSample
import io.cebes.df.schema.{Schema, SchemaField}
import io.cebes.df.types.StorageTypes
import io.cebes.server.routes.df.HttpDfJsonProtocol._
import org.scalatest.FunSuite
import spray.json._

/**
  * Test all kinds of serialization/deserialization
  */
class SerializationSuite extends FunSuite {

  test("DataSample empty case") {
    val sample = new DataSample(Schema(), Seq.empty[Seq[Any]])
    val s = sample.toJson.compactPrint

    val sample2 = s.parseJson.convertTo[DataSample]
    assert(sample2.schema === sample.schema)
    assert(sample2.data.isEmpty)
  }

  test("DataSample simple case") {
    val sc = Schema(Seq(
      new SchemaField("str_col", StorageTypes.StringType),
      new SchemaField("bool_col", StorageTypes.BooleanType),
      new SchemaField("byte_col", StorageTypes.ByteType),
      new SchemaField("short_col", StorageTypes.ShortType),
      new SchemaField("int_col", StorageTypes.IntegerType),
      new SchemaField("long_col", StorageTypes.LongType),
      new SchemaField("float_col", StorageTypes.FloatType),
      new SchemaField("double_col", StorageTypes.DoubleType)).toArray)
    val data = Seq(
      Seq("a", "b", null, "c"),
      Seq(false, true, null, false),
      Seq(1, 2, 3, null),
      Seq(null, 2, 3, 4),
      Seq(1, null, 3, 4),
      Seq(1, 2, 3, null),
      Seq(1.1f, 2.2f, 3, null),
      Seq(1.4, 2.5, 3, null)
    )
    val sample = new DataSample(sc, data)
    val s = sample.toJson.compactPrint

    val sample2 = s.parseJson.convertTo[DataSample]
    assert(sample2.schema === sample.schema)
    assert(sample2.data.zip(data).forall {
      case (s1, s2) => s1.zip(s2).forall { case (v1, v2) => v1 == v2 }
    })
  }
}
