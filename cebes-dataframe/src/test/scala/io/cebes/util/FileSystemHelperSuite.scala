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
 */
package io.cebes.util

import java.io._
import java.nio.file.{Files, Paths}

import org.scalatest.FunSuite

class FileSystemHelperSuite extends FunSuite {

  test("in-memory copyStream") {
    val data = Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9)
    ResourceUtil.using(new ByteArrayInputStream(data)) { inStream =>
      ResourceUtil.using(new ByteArrayOutputStream()) { outStream =>
        FileSystemHelper.copyStream(inStream, outStream)
        outStream.flush()
        assert(outStream.size() === data.length)
        assert(outStream.toByteArray.zip(data).forall { case (b1, b2) => b1 === b2 })
      }
    }
  }


  test("zipFolder and unzip") {
    val parentDir = Files.createTempDirectory("test-zip-folder")
    assert(new File(parentDir.toString, "a").mkdir())
    assert(new File(parentDir.toString, "a/a1").mkdir())
    assert(new File(parentDir.toString, "b").mkdir())
    val sampleData = Array[Byte](9, 8, 7, 6, 5, 4, 3, 2, 11, 1, 1, 1)
    ResourceUtil.using(new FileOutputStream(new File(parentDir.toString, "a/f1.txt"))) { f =>
      f.write(sampleData)
    }
    ResourceUtil.using(new FileOutputStream(new File(parentDir.toString, "b/f2.txt"))) {
      f => f.write(sampleData)
    }

    // zip
    val zipFile = Files.createTempFile("test-zip-folder-output", "")
    FileSystemHelper.zipFolder(parentDir.toString, zipFile.toString)
    FileSystemHelper.deleteRecursively(parentDir.toFile)

    // unzip
    val outFolder = Files.createTempDirectory("test-unzip")
    FileSystemHelper.unzip(zipFile.toString, outFolder.toString)

    // check some file contents
    val arr1 = Files.readAllBytes(Paths.get(outFolder.toString, "a/f1.txt"))
    assert(arr1.length === sampleData.length)
    assert(arr1.zip(sampleData).forall { case (b1, b2) => b1 === b2 })

    val arr2 = Files.readAllBytes(Paths.get(outFolder.toString, "b/f2.txt"))
    assert(arr2.length === sampleData.length)
    assert(arr2.zip(sampleData).forall { case (b1, b2) => b1 === b2 })

    // check folder a/a1 exists
    assert(Paths.get(outFolder.toString, "a/a1").toFile.isDirectory)

    FileSystemHelper.deleteRecursively(zipFile.toFile)
    FileSystemHelper.deleteRecursively(outFolder.toFile)
  }
}
