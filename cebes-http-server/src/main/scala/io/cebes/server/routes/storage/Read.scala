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
 * Created by phvu on 17/09/16.
 */

package io.cebes.server.routes.storage

import java.util.Base64

import com.google.inject.Inject
import io.cebes.df.Dataframe
import io.cebes.server.result.ResultStorage
import io.cebes.server.routes.common.AsyncDataframeOperation
import io.cebes.spark.storage.hdfs.HdfsDataSource
import io.cebes.spark.storage.rdbms.{HiveDataSource, JdbcDataSource}
import io.cebes.spark.storage.s3.S3DataSource
import io.cebes.storage.StorageService
import io.cebes.storage.localfs.LocalFsDataSource

import scala.concurrent.{ExecutionContext, Future}

class Read @Inject()(storageService: StorageService, override val resultStorage: ResultStorage)
  extends AsyncDataframeOperation[ReadRequest] {

  /**
    * Implement this to do the real work
    */
  override def runImpl(requestEntity: ReadRequest)(implicit ec: ExecutionContext): Future[Dataframe] = Future {

    val dataSrc = requestEntity match {
      case ReadRequest(Some(localFs), None, None, None, None, _) =>
        new LocalFsDataSource(localFs.path, localFs.format)
      case ReadRequest(None, Some(s3), None, None, None, _) =>
        new S3DataSource(s3.accessKey, s3.secretKey, s3.regionName, s3.bucketName, s3.key, s3.format)
      case ReadRequest(None, None, Some(hdfs), None, None, _) =>
        new HdfsDataSource(hdfs.path, hdfs.uri, hdfs.format)
      case ReadRequest(None, None, None, Some(jdbc), None, _) =>
        val jdbcPwd = new String(Base64.getUrlDecoder.decode(jdbc.passwordBase64), "UTF-8")
        JdbcDataSource(jdbc.url, jdbc.tableName, jdbc.userName, jdbcPwd, jdbc.driver)
      case ReadRequest(None, None, None, None, Some(hive), _) =>
        new HiveDataSource(hive.tableName)
      case _ => throw new IllegalArgumentException("Invalid read request")
    }
    storageService.read(dataSrc, requestEntity.readOptions)
  }
}
