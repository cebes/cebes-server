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
package io.cebes.repository.http

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Route
import io.cebes.http.client.ServerException
import io.cebes.repository.CebesRepositoryJsonProtocol._
import io.cebes.repository.RepositoryListResponse
import io.cebes.repository.db.Repository
import io.cebes.repository.inject.CebesRepositoryInjector
import org.scalatest.FunSuite

class CebesRepositoryServerSuite extends FunSuite with TestClient {
  private val server = CebesRepositoryInjector.instance[CebesRepositoryServer]

  override protected val serverRoutes: Route = server.routes
  override protected val apiVersion: String = CebesRepositoryServer.API_VERSION

  test("full suite") {
    val ex1 = intercept[ServerException] {
      get[RepositoryListResponse]("catalog/")
    }
    assert(ex1.message.startsWith("HttpResponse(308 Permanent Redirect,List(Location: catalog/0)"))

    val repos = get[RepositoryListResponse]("catalog/0")
    assert(repos.repositories.isEmpty)
    assert(repos.pageId === 0 && repos.totalPages === 0)

    val r = put[String, Repository]("repos/abcd", "")

  }
}
