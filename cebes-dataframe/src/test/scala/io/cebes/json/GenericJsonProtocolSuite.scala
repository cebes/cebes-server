/* Copyright 2017 The Cebes Authors. All Rights Reserved.
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
package io.cebes.json

import org.scalatest.FunSuite
import spray.json._

/**
  * Special suite for [[GenericJsonProtocol]]
  */
class GenericJsonProtocolSuite extends FunSuite with GenericJsonProtocol {

  test("writeJson and readJson") {
    // String[]
    val arr1 = Array[String]("s1", "s2", "s3")
    val sJs = writeJson(arr1).compactPrint
    //println(sJs): {"type":"array","data":["s1","s2","s3"]}

    val arr2 = readJson(sJs.parseJson)
    assert(arr2.isInstanceOf[Array[_]])
    assert(arr2.asInstanceOf[Array[_]].length === 3)
    assert(arr2.asInstanceOf[Array[_]](0) === "s1")
  }
}
