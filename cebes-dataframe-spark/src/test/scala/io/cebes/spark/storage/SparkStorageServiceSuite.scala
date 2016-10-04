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

import io.cebes.spark.helpers.{TestDataHelper, TestPropertyHelper}
import io.cebes.spark.storage.rdbms.{HiveDataSource, JdbcDataSource}
import io.cebes.spark.storage.s3.S3DataSource
import io.cebes.storage.DataFormats
import io.cebes.storage.localfs.LocalFsDataSource
import io.cebes.util.ResourceUtil
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SparkStorageServiceSuite extends FunSuite with BeforeAndAfterAll with TestPropertyHelper with TestDataHelper {

  val cylinderBandsTableName = "cylinder_bands_storage_test"

  override def beforeAll(): Unit = {
    super.beforeAll()
    createOrReplaceCylinderBands(cylinderBandsTableName)
  }

  test("Read CSV from S3", S3TestsEnabled) {
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

  test("Read/write data from/to Hive") {
    val df = sparkStorageService.read(new HiveDataSource(cylinderBandsTableName))
    assert(df.numCols === 40)
    assert(df.numRows === 541)

    // TODO: the results of this operation can only be tested once we have head() for Dataframe
    //val newTableName = "cylinder_bands_new_table"
    //sparkStorageService.write(df, new HiveDataSource(newTableName))

  }

  test("Read/write data from/to JDBC", JdbcTestsEnabled) {
    val df = sparkStorageService.read(new HiveDataSource(cylinderBandsTableName))
    assert(df.numCols === 40)
    assert(df.numRows === 541)

    val jdbcSrc = new JdbcDataSource(properties.jdbcUrl, "cylinder_bands_test_table",
      properties.jdbcUsername, properties.jdbcPassword)

    try {
      val df2 = sparkStorageService.read(jdbcSrc)
      assert(df2.numCols === 40)
      assert(df2.numRows === 541)
    } catch {
      case ex: org.postgresql.util.PSQLException =>
        sparkStorageService.write(df, jdbcSrc)
        val df2 = sparkStorageService.read(jdbcSrc)
        assert(df2.numCols === 40)
        assert(df2.numRows === 541)
    }
  }

  test("Read/write data from/to HDFS") {
    // TODO: implement this
  }
}
