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
 * Created by phvu on 24/08/16.
 */

package io.cebes.http.server.auth

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import io.cebes.auth.AuthService
import io.cebes.http.server.auth.HttpAuthJsonProtocol._
import io.cebes.http.server.routes.{SecuredSession, SessionData}

/**
  * Handle all authentication requests
  */
trait AuthHandler extends SecuredSession {

  protected val authService: AuthService

  val authApi: Route = pathPrefix("auth") {
    (path("login") & post) {
      entity(as[LoginRequest]) { userLogin =>
        if (authService.login(userLogin.userName, userLogin.passwordHash)) {
          setCebesSession(SessionData(userLogin.userName)) {
            setCebesCsrfToken() { ctx =>
              ctx.complete(LoginResponse("ok"))
            }
          }
        } else {
          throw new IllegalArgumentException("Invalid username or password")
        }
      }
    } ~ (path("logout") & post) {
      requiredCebesSession { _ =>
        invalidateCebesSession { ctx =>
          ctx.complete(LoginResponse("ok"))
        }
      }
    }
  }
}
