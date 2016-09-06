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

import java.io.ByteArrayInputStream

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model._
import io.cebes.storage.DataWriter

import scala.collection.{JavaConversions, mutable}

/**
  * This class is stateful, as we do multipart upload
  *
  * @param s3Client   the S3 client
  * @param bucketName bucket name
  * @param key        key to the object
  */
class S3DataWriter(val s3Client: AmazonS3Client,
                   val bucketName: String, val key: String) extends DataWriter {

  private var partNumber: Int = 1
  private val uploadId = s3Client.initiateMultipartUpload(
    new InitiateMultipartUploadRequest(bucketName, key)).getUploadId
  private val partETags = new mutable.MutableList[PartETag]()

  /**
    * Append some bytes into the current file
    *
    * @param bytes the bytes to be written
    * @return the number of bytes have been really written
    */
  override def append(bytes: Array[Byte]): Int = {
    if (partNumber < 0) {
      throw new IllegalArgumentException("Last request was failed. Cannot continue writing on this writer")
    }

    try {
      val uploadRequest = new UploadPartRequest()
        .withBucketName(bucketName).withKey(key)
        .withUploadId(uploadId)
        .withPartNumber(partNumber)
        .withInputStream(new ByteArrayInputStream(bytes))
        .withPartSize(bytes.length)

      partETags += s3Client.uploadPart(uploadRequest).getPartETag
      partNumber += 1
      bytes.length
    } catch {
      case e: Exception =>
        partNumber = -1
        throw e
    }
  }

  /**
    * Close the writer, release resources
    */
  override def close(): Unit = {
    if (partNumber < 0) {
      s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(
        bucketName, key, uploadId))
    } else {
      s3Client.completeMultipartUpload(
        new CompleteMultipartUploadRequest(bucketName,
          key, uploadId,
          JavaConversions.mutableSeqAsJavaList(partETags)))
    }
  }
}
