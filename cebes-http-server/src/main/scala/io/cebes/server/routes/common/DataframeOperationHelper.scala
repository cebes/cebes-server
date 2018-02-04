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
package io.cebes.server.routes.common

import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.FromRequestUnmarshaller
import io.cebes.http.server.operations.OperationHelper
import io.cebes.server.routes.common.HttpServerJsonProtocol._
import spray.json.JsonFormat

import scala.reflect.ClassTag

/**
  * Common helpers for [[io.cebes.df.Dataframe]]-related operations
  */
trait DataframeOperationHelper extends OperationHelper {

  /**
    * An operation done by class [[W]] (subclass of [[AsyncDataframeOperation]],
    * with entity of type [[E]]
    */
  protected def operationDf[W <: AsyncDataframeOperation[E], E]
  (implicit tag: ClassTag[W], umE: FromRequestUnmarshaller[E], jfE: JsonFormat[E]): Route = {
    operation[W, E, DataframeResponse]
  }

}
