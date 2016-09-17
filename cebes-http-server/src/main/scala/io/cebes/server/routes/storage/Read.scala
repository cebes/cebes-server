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

import io.cebes.df.Dataframe
import io.cebes.server.common.AsyncExecutor
import io.cebes.server.models.{DataframeResponse, ReadRequest}
import io.cebes.spark.storage.hdfs.HdfsDataSource
import io.cebes.spark.storage.rdbms.{HiveDataSource, JdbcDataSource}
import io.cebes.spark.storage.s3.S3DataSource
import io.cebes.storage.localfs.LocalFsDataSource
import io.cebes.storage.{DataFormat, StorageService}

import scala.concurrent.{ExecutionContext, Future}

class Read(storageService: StorageService)
  extends AsyncExecutor[ReadRequest, Dataframe, DataframeResponse] {

  /**
    * Implement this to do the real work
    */
  override def runImpl(requestEntity: ReadRequest)
                      (implicit ec: ExecutionContext): Future[Dataframe] = Future {

    val dataSrc = requestEntity match {
      case ReadRequest(Some(localFs), None, None, None, None) =>
        new LocalFsDataSource(localFs.path, localFs.format)
      case ReadRequest(None, Some(s3), None, None, None) =>
        new S3DataSource(s3.accessKey, s3.secretKey,
          s3.bucketName, s3.key, s3.format)
      case ReadRequest(None, None, Some(hdfs), None, None) =>
        new HdfsDataSource(hdfs.path, hdfs.uri, hdfs.format)
      case ReadRequest(None, None, None, Some(jdbc), None) =>
        new JdbcDataSource(jdbc.url, jdbc.tableName, jdbc.userName, jdbc.passwordBase64, DataFormat.Unknown)
      case ReadRequest(None, None, None, None, Some(hive)) =>
        new HiveDataSource(hive.tableName, DataFormat.Unknown)
      case _ => throw new IllegalArgumentException("Invalid read request")
    }
    storageService.read(dataSrc)
  }


  /**
    * Transform the actual result (of type T)
    * into something that will be returned to the clients
    * Normally R should be Json-serializable.
    *
    * @param requestEntity The request entity
    * @param result        The actual result, returned by `runImpl`
    * @return a JSON-serializable object, to be returned to the clients
    */
  override def transformResult(requestEntity: ReadRequest, result: Dataframe): DataframeResponse = {
    DataframeResponse(result.id)
  }
}
