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

package io.cebes.json

import java.sql.Timestamp
import java.util.{Calendar, Date}

import io.cebes.df.expressions.Expression
import io.cebes.df.sample.DataSample
import io.cebes.df.schema.{Schema, SchemaField}
import io.cebes.df.types.storage._
import io.cebes.df.types.{StorageTypes, VariableTypes}
import io.cebes.df.{expressions => exprs}
import io.cebes.json.CebesCoreJsonProtocol._
import io.cebes.json.CebesExpressionDefaultJsonProtocol._
import org.scalatest.FunSuite
import spray.json._

import scala.collection.mutable

/**
  * Test all kinds of serialization/deserialization
  */
class CebesJsonProtocolSuite extends FunSuite {

  test("Metadata") {
    val metadata = new MetadataBuilder()
      .putDouble("dbl", 150.0).putNull("null").putLong("long", 160)
      .putBoolean("bool", value = false).putString("str", "blah blah")
      .putMetadata("metadata", new MetadataBuilder().putNull("empty").build())
      .putDoubleArray("dbl_array", Array(1.4, 1.5, 1.6))
      .putLongArray("long_array", Array(1, 2, 3, 4))
      .putBooleanArray("bool_array", Array(true, false, true, true))
      .putStringArray("str_array", Array("s1", "s2", "s3"))
      .putMetadataArray("metadata_array", Array(new MetadataBuilder().putNull("empty").build()))
      .build()

    val s = metadata.toJson.compactPrint
    val metadata2 = s.parseJson.convertTo[Metadata]
    assert(metadata2.equals(metadata2))
  }

  test("Schema") {
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

  test("DataSample binary column") {
    val sc = Schema(Seq(new SchemaField("binary_col", StorageTypes.BinaryType)).toArray)
    val data = Seq(Seq(Array[Byte](1, 2, 3), Array[Byte](), null, Array[Byte](1, 5, 4, 100)))
    val sample = new DataSample(sc, data)
    val s = sample.toJson.compactPrint

    val sample2 = s.parseJson.convertTo[DataSample]
    assert(sample2.schema === sample.schema)
    assert(data.zip(sample2.data).forall {
      case (s1, s2) => s1.zip(s2).forall {
        case (null, null) => true
        case (v1: Array[_], v2: Array[_]) => v1.sameElements(v2)
      }
    })
  }

  test("DataSample datetime") {
    val sc = Schema(Seq(
      new SchemaField("col1", StorageTypes.DateType),
      new SchemaField("col2", StorageTypes.TimestampType)
    ).toArray)

    val data = Seq(
      Seq(Calendar.getInstance().getTime, null),
      Seq(new Timestamp(Calendar.getInstance().getTimeInMillis)), null)

    val sample = new DataSample(sc, data)
    val s = sample.toJson.compactPrint

    val sample2 = s.parseJson.convertTo[DataSample]
    assert(sample2.schema === sample.schema)
    assert(sample2.get[Date]("col1").get.zip(sample.get[Date]("col1").get).forall {
      case (v2, v1) => v2 === v1
    })
    assert(sample2.get[Timestamp]("col2").get.zip(sample.get[Timestamp]("col2").get).forall {
      case (v2, v1) => v2 === v1
    })
  }

  test("DataSample array") {
    val sc = Schema(Seq(
      new SchemaField("col1", StorageTypes.arrayType(StorageTypes.FloatType)),
      new SchemaField("col2", StorageTypes.arrayType(StorageTypes.arrayType(StorageTypes.IntegerType)))
    ).toArray)

    val data = Seq(
      Seq(mutable.WrappedArray.make[Float](Array[Float](1.0f, 2.0f, 4.0f))),
      Seq(mutable.WrappedArray.make[mutable.WrappedArray[Int]](
        Array[mutable.WrappedArray[Int]](
          mutable.WrappedArray.make[Int](Array(3, 4, 5)),
          null,
          mutable.WrappedArray.make[Int](Array(9, 10, 11))
        )
      )))

    val sample = new DataSample(sc, data)
    val s = sample.toJson.compactPrint

    val sample2 = s.parseJson.convertTo[DataSample]
    assert(sample2.schema === sample.schema)
    assert(sample2.get[mutable.WrappedArray[Float]]("col1").get.zip(
      sample.get[mutable.WrappedArray[Float]]("col1").get).forall { case (v2, v1) => v2 === v1 })
    assert(sample2.get[mutable.WrappedArray[mutable.WrappedArray[Int]]]("col2").get.zip(
      sample.get[mutable.WrappedArray[mutable.WrappedArray[Int]]]("col2").get).forall {
      case (v2, v1) => v2 === v1
    })
  }

  test("DataSample map/struct") {
    val sc = Schema(Seq(
      new SchemaField("map_col1", StorageTypes.mapType(StorageTypes.StringType, StorageTypes.FloatType)),
      new SchemaField("map_col2",
        StorageTypes.mapType(StorageTypes.StringType, StorageTypes.arrayType(StorageTypes.IntegerType)))
    ).toArray)
    val data = Seq(
      Seq(Map("e1" -> 10.0f, "e" -> 20.1f), Map(), null),
      Seq(Map("e1" -> mutable.WrappedArray.make(Array[Int](1, 5, 4)), "e" -> null), Map(), null)
    )
    val sample = new DataSample(sc, data)
    val s = sample.toJson.compactPrint

    val sample2 = s.parseJson.convertTo[DataSample]
    assert(sample2.schema === sample.schema)
    assert(sample2.get[Map[String, Float]]("map_col1").get.zip(
      sample.get[Map[String, Float]]("map_col1").get).forall {
      case (v2, v1) => v2 === v1
    })
    assert(sample2.get[Map[String, mutable.WrappedArray[Int]]]("map_col2").get.zip(
      sample.get[Map[String, mutable.WrappedArray[Int]]]("map_col2").get).forall {
      case (v2, v1) => v2 === v1
    })
  }

  test("Expression") {
    val abs: exprs.Expression = exprs.Abs(exprs.Literal("100"))
    assert(abs.name === "Abs")
    val js = abs.toJson.compactPrint

    val abs2 = js.parseJson.convertTo[Expression]
    assert(abs2.isInstanceOf[exprs.Abs])
    assert(abs2.asInstanceOf[exprs.Abs].child.asInstanceOf[exprs.Literal].value === "100")

    // with variable number of arguments
    val c1: exprs.Expression = exprs.CountDistinct(exprs.Literal("a"), exprs.Literal("b"), exprs.Literal("c"))
    val js2 = c1.toJson.compactPrint

    val c2 = js2.parseJson.convertTo[Expression]
    assert(c2.isInstanceOf[exprs.CountDistinct])
    assert(c2 === c1)
    val c3 = c2.asInstanceOf[exprs.CountDistinct]
    assert(c3.children.length === 3)
    assert(c3.children.head.asInstanceOf[exprs.Literal].value === "a")
    assert(c3.children(1).asInstanceOf[exprs.Literal].value === "b")
    assert(c3.children(2).asInstanceOf[exprs.Literal].value === "c")

    // Seq[String]
    val m1: exprs.Expression = exprs.MultiAlias(exprs.Literal(100), Seq("a", "b", "c"))
    val js3 = m1.toJson.compactPrint

    val m2 = js3.parseJson.convertTo[exprs.Expression]
    assert(m2.isInstanceOf[exprs.MultiAlias])
    assert(m2 === m1)

    // Storage type
    val c4: exprs.Expression = exprs.Cast(exprs.Literal("aaaa"), StorageTypes.DateType)
    val js4 = c4.toJson.compactPrint

    val c5 = js4.parseJson.convertTo[exprs.Expression]
    assert(c5.isInstanceOf[exprs.Cast])
    assert(c5 === c4)

    // Seq[Expression]
    val createArray: exprs.Expression = exprs.CreateArray(
      Seq(exprs.Literal("a"), exprs.Literal("b"), exprs.Literal("c")))
    val createArrayStr = createArray.toJson.compactPrint

    val createArray2 = createArrayStr.parseJson.convertTo[exprs.Expression]
    assert(createArray2.isInstanceOf[exprs.CreateArray])
    assert(createArray2 === createArray)

    // CaseWhen
    val caseWhen: exprs.Expression = exprs.CaseWhen(
      Seq((exprs.Literal("a"), exprs.Literal(100)),
        (exprs.Literal("b"), exprs.Literal(200))),
      Some(exprs.Literal(300)))
    val caseWhenStr = caseWhen.toJson.compactPrint

    val caseWhen2 = caseWhenStr.parseJson.convertTo[exprs.Expression]
    assert(caseWhen2.isInstanceOf[exprs.CaseWhen])
    assert(caseWhen2 === caseWhen)

    // CaseWhen big
    val caseWhenBig: exprs.Expression = exprs.CaseWhen(
      Seq((exprs.Literal("a"), exprs.MultiAlias(exprs.Literal(100), Seq("a", "b", "c"))),
        (exprs.Literal("b"), exprs.Cast(exprs.Literal("aaaa"), StorageTypes.DateType))),
      None)
    val caseWhenBigStr = caseWhenBig.toJson.compactPrint

    val caseWhenBig2 = caseWhenBigStr.parseJson.convertTo[exprs.Expression]
    assert(caseWhenBig2.isInstanceOf[exprs.CaseWhen])
    assert(caseWhenBig2 === caseWhenBig)
  }
}
