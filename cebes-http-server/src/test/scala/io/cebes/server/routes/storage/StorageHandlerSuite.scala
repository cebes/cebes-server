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
import io.cebes.server.helpers.TestPropertyHelper
import io.cebes.server.models.CebesJsonProtocol._
import io.cebes.server.models.{DataframeResponse, ReadRequest, S3ReadRequest}
import io.cebes.server.routes.AbstractRouteSuite
import io.cebes.storage.DataFormats

class StorageHandlerSuite extends AbstractRouteSuite with TestPropertyHelper {

  test("read data from S3", S3TestsEnabled) {
    val result = waitDf(postAsync[ReadRequest, DataframeResponse]("storage/read",
      ReadRequest(None, Some(S3ReadRequest(properties.awsAccessKey, properties.awsSecretKey,
        Some("us-west-1"), "cebes-data-test", "read/cylinder_bands.csv", DataFormats.CSV)),
        None, None, None)))
    assert(result.id.toString.length > 0)
  }
}
