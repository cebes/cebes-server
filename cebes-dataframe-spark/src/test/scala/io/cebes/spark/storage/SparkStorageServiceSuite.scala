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
 * Created by phvu on 22/09/16.
 */

package io.cebes.spark.storage

import io.cebes.spark.CebesSparkTestInjector
import io.cebes.spark.helpers.HasTestProperties
import io.cebes.spark.storage.s3.S3DataSource
import io.cebes.storage.DataFormat
import org.scalatest.FunSuite

class SparkStorageServiceSuite extends FunSuite with HasTestProperties {

  val sparkStorageService = CebesSparkTestInjector.injector.getInstance(classOf[SparkStorageService])

  test("Read CSV from S3") {
    val df = sparkStorageService.read(new S3DataSource(properties.awsAccessKey,
      properties.awsSecretKey, Some("us-west-1"), "cebes-data-test", "cylinder_bands.csv", DataFormat.CSV))
    assert(df.numCols === 40)
    assert(df.numRows === 541)
  }
}
