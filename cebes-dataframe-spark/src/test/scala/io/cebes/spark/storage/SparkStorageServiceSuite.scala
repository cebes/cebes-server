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

import java.nio.file.{FileVisitOption, Files, Path, Paths}
import java.util.Comparator
import java.util.function.Consumer

import io.cebes.spark.helpers.{HasTestProperties, TestHelper}
import io.cebes.spark.storage.s3.S3DataSource
import io.cebes.storage.DataFormats
import io.cebes.storage.localfs.LocalFsDataSource
import io.cebes.util.ResourceUtil
import org.scalatest.FunSuite

class SparkStorageServiceSuite extends FunSuite with HasTestProperties with TestHelper {

  test("Read CSV from S3") {
    val s3ReadSrc = new S3DataSource(properties.awsAccessKey, properties.awsSecretKey,
      Some("us-west-1"), "cebes-data-test", "read/cylinder_bands.csv", DataFormats.CSV)
    val df = sparkStorageService.read(s3ReadSrc)
    assert(df.numCols === 40)
    assert(df.numRows === 541)
  }

  test("Read/write data from/to local storage") {
    val file = ResourceUtil.getResourceAsFile("/data/cylinder_bands.csv")
    val df = sparkStorageService.read(new LocalFsDataSource(file.getAbsolutePath, DataFormats.CSV))
    assert(df.numCols === 40)
    assert(df.numRows === 541)

    val tmpDir = Files.createTempDirectory(s"cebes_test")
    Seq(DataFormats.CSV, DataFormats.JSON, DataFormats.PARQUET).foreach { fmt =>
      val f = Paths.get(tmpDir.toString, fmt.name)

      val localSource = new LocalFsDataSource(f.toString, fmt)
      sparkStorageService.write(df, localSource)

      assert(Files.exists(f))
      Files.walk(f, FileVisitOption.FOLLOW_LINKS)
        .sorted(Comparator.reverseOrder())
        .forEach(new Consumer[Path] {
          override def accept(t: Path): Unit = Files.delete(t)
        })
      assert(!Files.exists(f))
    }
    Files.delete(tmpDir)
  }
}
