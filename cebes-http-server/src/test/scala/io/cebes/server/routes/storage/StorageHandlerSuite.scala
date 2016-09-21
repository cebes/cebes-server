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
 * Created by phvu on 17/09/16.
 */

package io.cebes.server.routes.storage

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.HttpMethods
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.cebes.server.helpers.{HasClient, HasTestProperties}
import io.cebes.server.models.CebesJsonProtocol._
import io.cebes.server.models.{FutureResult, ReadRequest, S3ReadRequest}
import io.cebes.storage.DataFormat

import scala.concurrent.ExecutionContext.Implicits.global

class StorageHandlerSuite extends HasClient with HasTestProperties with StrictLogging {

  test("read data from S3") {
    val futureResult = client.request[ReadRequest, FutureResult](HttpMethods.POST, "storage/read",
      ReadRequest(None,
        Some(S3ReadRequest(properties.awsAccessKey, properties.awsSecretKey,
          Some("us-west-1"), "cebes-data-test", "cylinder_bands.csv", DataFormat.CSV)), None, None, None))

    val result = client.wait(futureResult)
    println(result.requestId.toString)
    println(result.status.toString)
    println(result.response.map(_.prettyPrint))
  }
}
