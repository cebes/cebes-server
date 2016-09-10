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
 * Created by phvu on 31/08/16.
 */

package io.cebes.spark.storage.s3

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import io.cebes.storage.DataFormat.DataFormatEnum
import io.cebes.storage.{DataSource, DataWriter}

class S3DataSource(val awsAccessKey: String, val awsSecretKey: String,
                   val bucketName: String, val key: String,
                   val format: DataFormatEnum) extends DataSource {

  private val s3client = new AmazonS3Client(new BasicAWSCredentials(awsAccessKey, awsSecretKey))

  def fullUrl: String = {
    s3client.getResourceUrl(bucketName, key)
  }

  /**
    * Open a data writer on this source, normally a file
    *
    * @param overwrite when a file exists, overwrite it if overwrite = true,
    *                  or throw an exception otherwise
    * @return a [[DataWriter]] object
    */
  override def open(overwrite: Boolean): DataWriter = {
    if (!s3client.doesBucketExist(bucketName)) {
      s3client.createBucket(bucketName)
    }
    val newKey = DataSource.validateFileName(key,
      s3client.doesObjectExist(bucketName, _),
      isFile = true, isDirectory = false, overwrite)
    s3client.getResourceUrl(bucketName, key)
    new S3DataWriter(s3client, bucketName, newKey)
  }
}
