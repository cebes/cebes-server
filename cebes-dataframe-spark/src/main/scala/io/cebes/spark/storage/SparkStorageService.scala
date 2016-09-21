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
 * Created by phvu on 05/09/16.
 */

package io.cebes.spark.storage

import java.util.Properties

import com.google.inject.Inject
import io.cebes.df.Dataframe
import io.cebes.spark.config.HasSparkSession
import io.cebes.spark.df.SparkDataframe
import io.cebes.spark.storage.hdfs.HdfsDataSource
import io.cebes.spark.storage.rdbms.{HiveDataSource, JdbcDataSource}
import io.cebes.spark.storage.s3.S3DataSource
import io.cebes.spark.util.CebesSparkUtil
import io.cebes.storage.localfs.LocalFsDataSource
import io.cebes.storage.{DataFormat, DataSource, StorageService}


/**
  * Implements [[StorageService]] on Spark
  */
class SparkStorageService @Inject()(hasSparkSession: HasSparkSession) extends StorageService {

  private val sparkSession = hasSparkSession.session

  /**
    * Write the given dataframe to the given datasource
    *
    * This is an internal API, designed mainly for cebes servers, not for end-users.
    *
    * @param dataframe  data frame to be written
    * @param dataSource data storage to store the given data frame
    */
  override def write(dataframe: Dataframe, dataSource: DataSource): Unit = {
    val sparkDf = CebesSparkUtil.getSparkDataframe(dataframe).sparkDf

    dataSource match {
      case jdbcSource: JdbcDataSource =>
        val prop = new Properties()
        prop.setProperty("user", jdbcSource.userName)
        prop.setProperty("password", jdbcSource.rawPassword)
        sparkDf.write.jdbc(jdbcSource.url, jdbcSource.tableName, prop)
      case hiveSource: HiveDataSource =>
        sparkDf.write.saveAsTable(hiveSource.tableName)
      case _ =>
        val srcPath = dataSource match {
          case localFsSource: LocalFsDataSource =>
            localFsSource.path
          case hdfsSource: HdfsDataSource =>
            hdfsSource.fullUrl
          case s3Source: S3DataSource =>
            s3Source.setUpSparkContext(sparkSession.sparkContext)
            s3Source.fullUrl
        }
        dataSource.format match {
          case DataFormat.CSV => sparkDf.write.csv(srcPath)
          case DataFormat.JSON => sparkDf.write.json(srcPath)
          case DataFormat.ORC => sparkDf.write.orc(srcPath)
          case DataFormat.PARQUET => sparkDf.write.parquet(srcPath)
          case DataFormat.TEXT => sparkDf.write.text(srcPath)
          case DataFormat.UNKNOWN => sparkDf.write.save(srcPath)
        }
    }
  }

  /**
    * Read the given data source
    *
    * This is end-user API.
    *
    * @param dataSource source to read data from
    * @return a new Dataframe
    */
  override def read(dataSource: DataSource): Dataframe = {
    val sparkDf = dataSource match {
      case jdbcSource: JdbcDataSource =>
        val prop = new Properties()
        prop.setProperty("user", jdbcSource.userName)
        prop.setProperty("password", jdbcSource.rawPassword)
        sparkSession.read.jdbc(jdbcSource.url, jdbcSource.tableName, prop)
      case hiveSource: HiveDataSource =>
        sparkSession.read.table(hiveSource.tableName)
      case _ =>
        val srcPath = dataSource match {
          case localFsSource: LocalFsDataSource =>
            localFsSource.path
          case hdfsSource: HdfsDataSource =>
            hdfsSource.fullUrl
          case s3Source: S3DataSource =>
            s3Source.setUpSparkContext(sparkSession.sparkContext)
            s3Source.fullUrl
        }
        dataSource.format match {
          case DataFormat.CSV => sparkSession.read.csv(srcPath)
          case DataFormat.JSON => sparkSession.read.json(srcPath)
          case DataFormat.ORC => sparkSession.read.orc(srcPath)
          case DataFormat.PARQUET => sparkSession.read.parquet(srcPath)
          case DataFormat.TEXT => sparkSession.read.text(srcPath)
          case DataFormat.UNKNOWN => sparkSession.read.load(srcPath)
        }
    }
    new SparkDataframe(sparkDf)
  }
}
