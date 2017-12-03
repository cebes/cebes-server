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
package io.cebes.serving.spark

import java.util.UUID

import com.google.inject.Inject
import io.cebes.pipeline.factory.PipelineFactory
import io.cebes.pipeline.models.Pipeline
import io.cebes.prop.types.MySqlBackendCredentials
import io.cebes.prop.{Prop, Property}
import io.cebes.serving.ServingConfiguration
import io.cebes.serving.common.ServingManager
import io.cebes.spark.pipeline.store.SparkPipelineStore
import io.cebes.store.TagStore

/**
  * Implementation of [[ServingManager]] on Spark
  * Serve as a store of pipelines being served, that can be looked-up using their servingNames.
  */
class SparkServingManager @Inject()
(servingConfiguration: ServingConfiguration,
 @Prop(Property.CACHESPEC_PIPELINE_STORE) cacheSpec: String,
 mySqlCreds: MySqlBackendCredentials,
 pplFactory: PipelineFactory,
 tagStore: TagStore[Pipeline])
  extends SparkPipelineStore(cacheSpec, mySqlCreds, pplFactory, tagStore) with ServingManager {

  private case class PipelineIndexInformation(id: UUID, slotNamings: Map[String, String])

  private lazy val servings: Map[String, PipelineIndexInformation] = loadPipelines()


  override def get(servingName: String): Option[PipelineInformation] = {
    servings.get(servingName).flatMap { pplInfo =>
      get(pplInfo.id).map{ ppl =>
        PipelineInformation(ppl, pplInfo.slotNamings)
      }
    }
  }

  private def loadPipelines(): Map[String, PipelineIndexInformation]= {
    servingConfiguration.pipelines.par.map { servingPl =>
      val ppl = add(downloadPipeline(servingPl.pipelineTag, servingPl.userName, servingPl.password))
      servingPl.servingName -> PipelineIndexInformation(ppl.id, servingPl.slotNamings)
    }.seq.toMap
  }

  private def downloadPipeline(tag: String, userName: Option[String], password: Option[String]): Pipeline = {
    // download the package
    // extract
    // import using PipelineFactory
    null
  }
}
