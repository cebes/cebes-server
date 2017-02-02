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
package io.cebes.pipeline.models

class StageFoo extends Stage {

  val strIn: InputSlot[String] = inputSlot[String]("strIn", "The input string", None)
  val out: OutputSlot[Array[Int]] = outputSlot[Array[Int]]("out", "The output integer", None)

  override protected def run(inputs: SlotValueMap): SlotValueMap = {
    assert(inputs.size == 1)
    assert(inputs(strIn).isInstanceOf[String])
    SlotValueMap(out, Array(2000))
  }
}

class StageFooNonDeterministic extends StageFoo {
  override def nonDeterministic: Boolean = true
}


class StageTwoInputs extends Stage {

  val valIn: InputSlot[Array[Int]] = inputSlot[Array[Int]]("valIn", "The input integer array", None)
  val m: InputSlot[String] = inputSlot[String]("m", "string input", None)

  val arrOut: OutputSlot[Array[Float]] = outputSlot[Array[Float]]("arrOut", "The output array", None)

  override protected def run(inputs: SlotValueMap): SlotValueMap = {
    assert(inputs.size == 2)
    assert(inputs(valIn).isInstanceOf[Array[Int]])
    assert(inputs(m).isInstanceOf[String])
    SlotValueMap(arrOut, Array(1.0f, 2.0f))
  }
}

class StageBar extends Stage {

  val strIn: InputSlot[String] = inputSlot[String]("strIn", "The input String", None)
  val m: OutputSlot[String] = outputSlot[String]("m", "string input", Some(""))

  override protected def run(inputs: SlotValueMap): SlotValueMap = {
    assert(inputs.size == 1)
    assert(inputs(strIn).isInstanceOf[String])
    SlotValueMap(m, "output of StageBar")
  }
}

class StageBadOutputType extends Stage {

  val m: OutputSlot[String] = outputSlot[String]("m", "string input", Some(""))

  override protected def run(inputs: SlotValueMap): SlotValueMap = {
    assert(inputs.isEmpty)
    SlotValueMap(m, 100)
  }
}

class StageBadOutputSize extends Stage {

  val m1: OutputSlot[String] = outputSlot[String]("m1", "string input", Some(""))
  val m2: OutputSlot[String] = outputSlot[String]("m2", "string input", Some(""))

  override protected def run(inputs: SlotValueMap): SlotValueMap = {
    assert(inputs.isEmpty)
    SlotValueMap(m1, "string 1")
  }
}
