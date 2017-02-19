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

package io.cebes.spark.storage.hdfs

import io.cebes.storage.DataWriter
import org.apache.hadoop.fs.FSDataOutputStream

class HdfsDataWriter(override val path: String,
                     val outStream: FSDataOutputStream) extends DataWriter {

  /**
    * Append some bytes into the current file
    *
    * @param bytes the bytes to be written
    * @return the number of bytes have been really written
    */
  override def append(bytes: Array[Byte]): Int = {
    outStream.write(bytes)
    bytes.length
  }

  /**
    * Close the writer, release resources
    */
  override def close(): Unit = outStream.close()
}
