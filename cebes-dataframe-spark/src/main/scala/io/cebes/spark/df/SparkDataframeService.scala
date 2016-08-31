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
 * Created by phvu on 25/08/16.
 */

package io.cebes.spark.df

import java.util.Properties

import io.cebes.df.{Dataframe, DataframeService}
import io.cebes.spark.storage.hdfs.HdfsDataSource
import io.cebes.spark.storage.rdbms.{HiveDataSource, JdbcDataSource}
import io.cebes.spark.storage.s3.S3DataSource
import io.cebes.storage.localfs.LocalFsDataSource
import io.cebes.storage.{DataFormat, DataSource, StorageService}
import org.apache.spark.sql.SparkSession

/**
  * Implements both [[DataframeService]] and [[StorageService]] on Spark,
  * because both of them will need the Spark session.
  */
class SparkDataframeService extends DataframeService with StorageService {

  val sparkSession = SparkSession.builder().appName("Cebes Dataframe service on Spark").getOrCreate()

  /**
    * Storage Service API
    */

  /**
    * Write the given dataframe to the given datasource
    *
    * This is an internal API, designed mainly for cebes servers, not for end-users.
    *
    * @param dataframe  data frame to be written
    * @param dataSource data storage to store the given data frame
    */
  override def write(dataframe: Dataframe, dataSource: DataSource): Unit = {
    val sparkDf = SparkDataframeService.getSparkDataframe(dataframe).sparkDf

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
            s3Source.fullUrl
        }
        dataSource.format match {
          case DataFormat.Csv => sparkDf.write.csv(srcPath)
          case DataFormat.Json => sparkDf.write.json(srcPath)
          case DataFormat.Orc => sparkDf.write.orc(srcPath)
          case DataFormat.Parquet => sparkDf.write.parquet(srcPath)
          case DataFormat.Text => sparkDf.write.text(srcPath)
          case DataFormat.Unknown => sparkDf.write.save(srcPath)
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
            s3Source.fullUrl
        }
        dataSource.format match {
          case DataFormat.Csv => sparkSession.read.csv(srcPath)
          case DataFormat.Json => sparkSession.read.json(srcPath)
          case DataFormat.Orc => sparkSession.read.orc(srcPath)
          case DataFormat.Parquet => sparkSession.read.parquet(srcPath)
          case DataFormat.Text => sparkSession.read.text(srcPath)
          case DataFormat.Unknown => sparkSession.read.load(srcPath)
        }
    }
    new SparkDataframe(sparkDf)
  }
}

object SparkDataframeService {

  def getSparkDataframe(df: Dataframe): SparkDataframe = df match {
    case sparkDf: SparkDataframe => sparkDf
    case _ => throw new IllegalArgumentException("Only SparkDataframe can be handled")
  }
}