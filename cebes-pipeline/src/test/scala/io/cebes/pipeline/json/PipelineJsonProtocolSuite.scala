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

import com.trueaccord.scalapb.GeneratedMessage
import io.cebes.pipeline.json.PipelineJsonProtocol._
import io.cebes.pipeline.protos.message.PipelineMessageDef
import io.cebes.pipeline.protos.param.ParamDef
import io.cebes.pipeline.protos.stage.StageDef
import io.cebes.pipeline.protos.value.{MapDef, ScalarDef, ValueDef}
import org.scalatest.FunSuite
import spray.json._

class PipelineJsonProtocolSuite extends FunSuite {

  test("ValueDef") {
    val value: GeneratedMessage = ValueDef().withScalar(ScalarDef().withDoubleVal(20.14))
    val s1 = value.toJson.compactPrint
    val value1 = s1.parseJson.convertTo[GeneratedMessage]
    assert(value1.isInstanceOf[ValueDef])
    assert(value === value1)

    val mapMsg: GeneratedMessage = ValueDef().withMap(MapDef().withEntry(Seq(
      MapDef.MapEntryDef().withKey(ValueDef().withScalar(ScalarDef().withStringVal("param100"))),
      MapDef.MapEntryDef().withKey(ValueDef().withScalar(ScalarDef().withDoubleVal(20.14)))
    )))
    val s2 = mapMsg.toJson.compactPrint
    val mapMsg1 = s2.parseJson.convertTo[GeneratedMessage]
    assert(mapMsg1.isInstanceOf[ValueDef])
    assert(mapMsg === mapMsg1)
  }

  test("StageDef") {
    val stageDef: GeneratedMessage = StageDef().withStage("StageClass").withName("stage1")
      .withInput(Map(0 -> "s1:0", 1 -> "s2:1"))
      .withOutput(Seq(PipelineMessageDef().withValue(ValueDef().withScalar(ScalarDef().withInt32Val(20)))))
      .withParam(Seq(
        ParamDef().withName("param1").withValue(ValueDef().withScalar(ScalarDef().withFloatVal(3.14f))),
        ParamDef().withName("param2").withValue(ValueDef().withScalar(ScalarDef().withInt64Val(194L)))))
    val s1 = stageDef.toJson.compactPrint
    val stageDef1 = s1.parseJson.convertTo[GeneratedMessage]
    assert(stageDef1.isInstanceOf[StageDef])
    assert(stageDef === stageDef1)
  }
}
