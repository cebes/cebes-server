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

package io.cebes.df.types.storage

import org.scalatest.FunSuite
import spray.json._
import io.cebes.df.schema.SchemaJsonProtocol._

class MetadataSuite extends FunSuite {

  test("simple operations") {
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

    assert(!metadata.getBoolean("bool"))
    assert(metadata.getDouble("dbl") === 150.0)
    assert(metadata.getLong("long") === 160)
    assert(metadata.getStringArray("str_array") === Array("s1", "s2", "s3"))
  }

  test("serialization/deserialization") {
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

}
