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
 * Created by phvu on 26/08/16.
 */

package io.cebes.storage

/**
  * Simple data writer
  */
trait DataWriter {

  /**
    * The exact path to the file written, maybe different from the path of the DataSource
    * (if DataSource points to an existing directory)
    *
    * @return the exact path where data was written
    */
  val path: String

  /**
    * Append some bytes into the current file
    *
    * @param bytes the bytes to be written
    * @return the number of bytes have been really written
    */
  def append(bytes: Array[Byte]): Int

  /**
    * Close the writer, release resources
    */
  def close(): Unit
}
