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
 * Created by phvu on 30/08/16.
 */

package io.cebes.storage.localfs

import java.io.{File, FileOutputStream}

import io.cebes.storage.DataWriter

/**
  * Data Writer that writes to local file system
  *
  * @param filePath the path to the underlying file
  */
class LocalFsDataWriter(val filePath: File) extends DataWriter {

  private val fileWriter = new FileOutputStream(filePath, false)

  /**
    * Append some bytes into the current file
    *
    * @param bytes the bytes to be written
    * @return the number of bytes have been really written
    */
  override def append(bytes: Array[Byte]): Int = {
    fileWriter.write(bytes)
    bytes.length
  }

  /**
    * Close the writer, release resources
    */
  override def close(): Unit = fileWriter.close()
}
