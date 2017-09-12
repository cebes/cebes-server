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
 *
 * Created by phvu on 18/12/2016.
 */

package io.cebes.server.routes.df

import com.google.inject.Inject
import io.cebes.df.{Dataframe, DataframeService}
import io.cebes.http.server.result.ResultStorage
import io.cebes.server.routes.common.AsyncDataframeOperation

import scala.concurrent.{ExecutionContext, Future}

class Replace @Inject()(dfService: DataframeService, override val resultStorage: ResultStorage)
  extends AsyncDataframeOperation[ReplaceRequest] {

  override protected def runImpl(requestEntity: ReplaceRequest)
                                (implicit ec: ExecutionContext): Future[Dataframe] = Future {
    require(requestEntity.replacement.nonEmpty,
      "Please provide a valid replacement map, with at least one mapping")

    val firstKey = requestEntity.replacement.keys.head
    require(requestEntity.replacement.keys.tail.forall(_.getClass == firstKey.getClass),
      "Only support replacement map of type String, Double or Boolean. Mixed types are not allowed")

    def toDouble(e: (Any, Any)): (Double, Double) = {
      (e._1.asInstanceOf[Number].doubleValue(),
        if (e._2 == null) Double.NaN else e._2.asInstanceOf[Number].doubleValue())
    }
    def toString(e: (Any, Any)): (String, String) = {
      (e._1.asInstanceOf[String], if (e._2 == null) null else e._2.asInstanceOf[String])
    }
    def toBoolean(e: (Any, Any)): (Boolean, Boolean) = {
      (e._1.asInstanceOf[Boolean], if (e._2 == null) false else e._2.asInstanceOf[Boolean])
    }

    firstKey match {
      case _: Number =>
        val map = requestEntity.replacement.map(toDouble)
        dfService.replace(requestEntity.df, requestEntity.colNames, map)
      case _: String =>
        val map = requestEntity.replacement.map(toString)
        dfService.replace(requestEntity.df, requestEntity.colNames, map)
      case _: Boolean =>
        val map = requestEntity.replacement.map(toBoolean)
        dfService.replace(requestEntity.df, requestEntity.colNames, map)
      case _ =>
        throw new IllegalArgumentException("Only support replacement map of type String, Double or Boolean")
    }

  }
}
