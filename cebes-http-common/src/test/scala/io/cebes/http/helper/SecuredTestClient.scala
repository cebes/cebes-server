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
package io.cebes.http.helper

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{HttpHeader, StatusCodes, headers => akkaHeaders}
import io.cebes.http.server.auth.HttpAuthJsonProtocol._
import io.cebes.http.server.auth.LoginRequest

/**
  * Extends [[TestClient]] to include login for authentication headers.
  * To be used to test secured server instances.
  */
trait SecuredTestClient extends TestClient {

  override protected lazy val authHeaders: Seq[HttpHeader] = login()

  private def login() = {
    Post(getUrl("auth/login"), LoginRequest("foo", "bar")) ~> serverRoutes ~> check {
      assert(status == StatusCodes.OK)
      val responseCookies = headers.filter(_.name().startsWith("Set-"))
      assert(responseCookies.nonEmpty)

      responseCookies.flatMap {
        case akkaHeaders.`Set-Cookie`(c) => c.name.toUpperCase() match {
          case "XSRF-TOKEN" =>
            Seq(akkaHeaders.RawHeader("X-XSRF-TOKEN", c.value), akkaHeaders.Cookie(c.name, c.value))
          case _ => Seq(akkaHeaders.Cookie(c.name, c.value))
        }
        case h =>
          Seq(akkaHeaders.RawHeader(h.name().substring(4), h.value()))
      }
    }
  }
}