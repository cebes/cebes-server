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
package io.cebes.spark.df.expressions

import java.util.UUID

import io.cebes.json.CebesCoreJsonProtocol._
import io.cebes.json.CebesExpressionJsonProtocol
import io.cebes.pipeline.json.PipelineJsonProtocol
import spray.json.DefaultJsonProtocol._
import spray.json._

/**
  * Implementation of [[PipelineJsonProtocol]] and [[CebesExpressionJsonProtocol]] on Spark, with additional
  * JsonFormats for [[SparkPrimitiveExpression]]
  */
trait CebesSparkJsonProtocol extends PipelineJsonProtocol {

  implicit val sparkPrimitiveExpressionFormat: RootJsonFormat[SparkPrimitiveExpression] =
    DefaultJsonProtocol.jsonFormat(
      (dfId: UUID, colName: String) => SparkPrimitiveExpression(dfId, colName, None), "dfId", "colName"
    )
}

object CebesSparkJsonProtocol extends CebesSparkJsonProtocol
