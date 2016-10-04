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

import java.nio.file.FileAlreadyExistsException

import io.cebes.storage.DataFormats.DataFormat

import scala.util.Random

trait DataSource {

  val format: DataFormat

  /**
    * Open a data writer on this source, normally a file
    *
    * @param overwrite when a file exists, overwrite it if overwrite = true,
    *                  or throw an exception otherwise
    * @return a [[DataWriter]] object
    */
  def open(overwrite: Boolean): DataWriter

}

object DataSource {

  /**
    * Validate a path, return a valid path to a file
    * If path exists and is a directory,
    * this function will generate a random file name
    *
    * @param path        path to check
    * @param exists      Function to check if a path exists
    * @param isFile      whether path is a File
    * @param isDirectory whether path is a Directory
    * @param overwrite   whether to overwrite the file if it exists
    * @return validated file path
    */
  def validateFileName(path: String,
                       exists: (String => Boolean), isFile: Boolean,
                       isDirectory: Boolean, overwrite: Boolean): String = {
    if (exists(path)) {
      if (isFile) {
        if (overwrite) {
          path
        } else {
          throw new FileAlreadyExistsException(s"File $path already exists")
        }
      } else if (isDirectory) {
        // make up a new file name
        val alphabet = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9') ++ "_"
        def generateName = (1 to 20).map(_ => alphabet(Random.nextInt(alphabet.size))).mkString
        val paths = for (_ <- (1 to 500).iterator) yield path ++ "/" ++ generateName
        val optFile = paths.find(!exists(_))
        if (optFile.isEmpty) {
          throw new IllegalArgumentException(s"Failed to randomly create a file under $path")
        } else {
          optFile.get
        }
      } else {
        throw new IllegalArgumentException(s"Path $path exists, and it is neither a file nor a directory")
      }
    } else {
      // path doesn't exists
      path
    }
  }
}
