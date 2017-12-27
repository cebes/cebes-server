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
package io.cebes.serving.inject

import java.io.File

import com.google.inject.{Inject, Provider}
import io.cebes.pipeline.json.ServingConfiguration
import io.cebes.prop.{Prop, Property}
import io.cebes.serving.common.DefaultPipelineJsonProtocol.servingConfigurationFormat
import io.cebes.util.ResourceUtil
import spray.json._

import scala.io.Source


/**
  * Guice provider for [[ServingConfiguration]]
  */
class ServingConfigurationProvider @Inject()(@Prop(Property.SERVING_CONFIG_FILE) configFile: String)
  extends Provider[ServingConfiguration] {

  override def get(): ServingConfiguration = {
    val realConfigFile = if (configFile.startsWith("/")) {
      ResourceUtil.getResourceAsFile(configFile)
    } else {
      new File(configFile)
    }
    if (!realConfigFile.exists()) {
      throw new IllegalArgumentException(s"Serving configuration file does not exist: ${realConfigFile.toString}")
    }
    ResourceUtil.using(Source.fromFile(realConfigFile)) { source =>
      source.mkString("").parseJson.convertTo[ServingConfiguration]
    }
  }
}
