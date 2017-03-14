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

package io.cebes.pipeline.json

import io.cebes.pipeline.json.PipelineJsonProtocol._
import org.scalatest.FunSuite
import spray.json._

class PipelineJsonProtocolSuite extends FunSuite {

  test("ValueDef") {
    val value = ValueDef(20.14)
    val s1 = value.toJson.compactPrint
    val value1 = s1.parseJson.convertTo[PipelineMessageDef]
    assert(value1.isInstanceOf[ValueDef])
    assert(value === value1)

    val mapMsg: PipelineMessageDef = ValueDef(Map("param100" -> 20.14))
    val s2 = mapMsg.toJson.compactPrint
    val mapMsg1 = s2.parseJson.convertTo[PipelineMessageDef]
    assert(mapMsg1.isInstanceOf[ValueDef])
    assert(mapMsg === mapMsg1)
  }

  test("StageDef") {
    val stageDef = StageDef("stage1", "StageClass",
      Map(
        "input0" -> StageOutputDef("s1", "out0"),
        "input1" -> StageOutputDef("s2", "out1"),
        "param1" -> ValueDef(3.14f),
        "param3" -> ValueDef(194L)
      ),
      Map(
        "out0" -> ValueDef(20)
      ))
    val s1 = stageDef.toJson.compactPrint
    val stageDef1 = s1.parseJson.convertTo[StageDef]
    assert(stageDef1.isInstanceOf[StageDef])
    assert(stageDef === stageDef1)
  }
}
