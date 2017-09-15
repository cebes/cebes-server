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
 * Created by phvu on 14/12/2016.
 */

package io.cebes.server.routes

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.server.Route
import io.cebes.pipeline.json.{ModelDef, PipelineDef}
import io.cebes.server.client.RemoteDataframe
import io.cebes.server.helpers.{CebesHttpServerTestInjector, TestClient}
import io.cebes.server.http.CebesHttpServer
import io.cebes.server.routes.common.DataframeResponse
import io.cebes.server.routes.df.DataframeRequest
import io.cebes.server.routes.test.HttpTestJsonProtocol._
import io.cebes.server.routes.test.{LoadDataRequest, LoadDataResponse}
import org.scalatest.FunSuite
import spray.json.DefaultJsonProtocol.LongJsonFormat
import spray.json._

/**
  * Mother of all Route test, with helpers for using akka test-kit,
  * logging in and storing cookies, etc...
  */
abstract class AbstractRouteSuite extends FunSuite with TestClient {

  private val server: CebesHttpServer = CebesHttpServerTestInjector.instance[CebesHttpServer]

  override protected val serverRoutes: Route = server.routes
  override protected val apiVersion: String = CebesHttpServer.API_VERSION

  lazy val getCylinderBands: RemoteDataframe = {
    val r = request[LoadDataRequest, LoadDataResponse]("test/loaddata", LoadDataRequest(Array("cylinder_bands")))
    val df1 = r.dataframes.head
    RemoteDataframe(df1.id, df1.schema)
  }

  protected def requestDf[E](url: String, entity: E)(implicit emE: ToEntityMarshaller[E],
                                                     jsDfr: JsonFormat[DataframeResponse]): RemoteDataframe = {
    val df = wait(postAsync[E, DataframeResponse](url, entity))
    RemoteDataframe(df.id, df.schema)
  }

  /**
    * Counting number of rows in the given dataframe
    */
  protected def count(df: RemoteDataframe)(implicit emE: ToEntityMarshaller[DataframeRequest]): Long = {
    request[DataframeRequest, Long]("df/count", DataframeRequest(df.id))
  }

  protected def requestPipeline[E](url: String, entity: E)(implicit emE: ToEntityMarshaller[E],
                                                           jsDfr: JsonFormat[PipelineDef]): PipelineDef = {
    wait(postAsync[E, PipelineDef](url, entity))
  }

  protected def requestModel[E](url: String, entity: E)(implicit emE: ToEntityMarshaller[E],
                                                        jsDfr: JsonFormat[ModelDef]): ModelDef = {
    wait(postAsync[E, ModelDef](url, entity))
  }
}
