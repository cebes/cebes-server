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
import io.cebes.pipeline.protos.value.{ScalarDef, ValueDef}
import org.scalatest.FunSuite
import spray.json._

class PipelineJsonProtocolSuite extends FunSuite {

  test("simple cases") {
    val value: GeneratedMessage = ValueDef().withScalar(ScalarDef().withDoubleVal(20.14))
    val s1 = value.toJson.compactPrint

    val value1 = s1.parseJson.convertTo[GeneratedMessage]
    assert(value1.isInstanceOf[ValueDef])
    assert(value === value1)
  }
}
