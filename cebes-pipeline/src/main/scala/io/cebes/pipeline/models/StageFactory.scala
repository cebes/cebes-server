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

import scala.concurrent.ExecutionContext
import scala.util.{Success, Try}

class StageFactory {

  private implicit val ec: ExecutionContext = ExecutionContext.global
  private val stageNamespaces: Seq[String] = Seq("io.cebes.pipeline.models")

  def create(proto: StageDef): Stage = {
    val cls = stageNamespaces.map { ns =>
      Try(Class.forName(s"$ns.${proto.stage}"))
    }.collectFirst {
      case Success(cl) if cl.getInterfaces.contains(classOf[Stage]) => cl
    } match {
      case Some(ns) => Class.forName(s"$ns.${proto.stage}")
      case None => throw new IllegalArgumentException(s"Stage class not found: ${proto.stage}")
    }
    cls.getConstructors.find { c =>
      c.getParameterCount == 2 && c.getParameterTypes.sameElements(Array(classOf[String], classOf[ExecutionContext]))
    } match {
      case Some(cl) => cl.newInstance(proto.name, ec).asInstanceOf[Stage]
      case None => throw new IllegalArgumentException(s"Failed to initialize stage class ${cls.getName}")
    }
  }
}
