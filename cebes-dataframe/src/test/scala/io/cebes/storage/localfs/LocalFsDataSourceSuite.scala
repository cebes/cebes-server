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
 * Created by phvu on 07/09/16.
 */

package io.cebes.storage.localfs

import java.io.IOException
import java.nio.file.{FileAlreadyExistsException, Files, Path, Paths}

import io.cebes.storage.DataFormat
import org.scalatest.FunSuite

class LocalFsDataSourceSuite extends FunSuite {

  test("open and write a file") {
    val pTmp = Files.createTempFile("cebes", "fsdatasource")
    val src = new LocalFsDataSource(pTmp.toAbsolutePath.toString, DataFormat.CSV)
    intercept[FileAlreadyExistsException] {
      src.open(false)
    }
    val writer = src.open(true)
    val arrData = Array(10, 20, 30, 40, 50).map(_.toByte)
    assert(writer.isInstanceOf[LocalFsDataWriter])
    assert(5 === writer.append(arrData))
    writer.close()
    intercept[IOException] {
      writer.append(Array(1, 2, 3).map(_.toByte))
    }

    assert(arrData.deep === Files.readAllBytes(pTmp))
    Files.delete(pTmp)
  }

  test("open and write a file in a directory") {
    val pTmp = Files.createTempDirectory("cebes")
    val src = new LocalFsDataSource(pTmp.toAbsolutePath.toString, DataFormat.CSV)
    val writer = src.open(false)
    val arrData = Array(10, 20, 30, 40, 50).map(_.toByte)
    assert(writer.isInstanceOf[LocalFsDataWriter])
    assert(5 === writer.append(arrData))
    writer.close()
    intercept[IOException] {
      writer.append(Array(1, 2, 3).map(_.toByte))
    }
    assert(pTmp.toString !== writer.path)
    assert(arrData.deep === Files.readAllBytes(Paths.get(writer.path)))
    Files.delete(Paths.get(writer.path))
    Files.delete(pTmp)
  }
}
