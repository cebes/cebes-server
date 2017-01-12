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

import io.cebes.pipeline.protos.stage.StageDef
import io.cebes.pipeline.protos.value.{ScalarDef, ValueDef}

import scala.util.{Success, Try}

class StageFactory {

  private val stageNamespaces: Seq[String] = Seq("io.cebes.pipeline.models")

  /**
    * Construct the stage object, given the proto
    * NOTE: current implementation doesn't consider the "output" in the proto.
    * It will ignore any value specified in the `output` field of the proto message.
    */
  def create(proto: StageDef): Stage = {

    // find the class
    val cls = stageNamespaces.map { ns =>
      Try(Class.forName(s"$ns.${proto.stage}"))
    }.collectFirst {
      case Success(cl) if cl.getInterfaces.contains(classOf[Stage]) => cl
    } match {
      case Some(ns) => Class.forName(s"$ns.${proto.stage}")
      case None => throw new IllegalArgumentException(s"Stage class not found: ${proto.stage}")
    }

    // construct the stage object, set the right name
    val stage = cls.getConstructors.find { c =>
      c.getParameterCount == 0
    } match {
      case Some(cl) => cl.newInstance().asInstanceOf[Stage].setName(proto.name)
      case None => throw new IllegalArgumentException(s"Failed to initialize stage class ${cls.getName}")
    }

    // set the params
    proto.param.foreach { p =>
      p.value.map { paramDef =>
        paramDef.value match {
          case ValueDef.Value.Scalar(scalarDef) =>
            scalarDef.value match {
              case ScalarDef.Value.StringVal(s) =>
                stage.set[String](stage.getParam(p.name).asInstanceOf[StringParam], s)
              case ScalarDef.Value.BoolVal(b) =>
                stage.set[Boolean](stage.getParam(p.name).asInstanceOf[BooleanParam], b)
              case ScalarDef.Value.Int32Val(i) =>
                stage.set[Int](stage.getParam(p.name).asInstanceOf[IntParam], i)
              case ScalarDef.Value.FloatVal(f) =>
                stage.set[Float](stage.getParam(p.name).asInstanceOf[FloatParam], f)
              case ScalarDef.Value.Int32Val(d) =>
                stage.set[Double](stage.getParam(p.name).asInstanceOf[DoubleParam], d)
              case scalaVal =>
                throw new UnsupportedOperationException("Unsupported scala value for stage parameters: " +
                s"Parameter name ${p.name} of stage ${proto.stage}(${proto.name}) " +
                  s"with value ${scalaVal.toString} of type ${scalaVal.getClass.getName}")
            }
          case valueDef =>
            throw new UnsupportedOperationException("Unsupported non-scala value for stage parameters: " +
              s"Parameter name ${p.name} of stage ${proto.stage}(${proto.name}) " +
              s"with value ${valueDef.toString} of type ${valueDef.getClass.getName}")
        }
      }
    }
    stage
  }
}
