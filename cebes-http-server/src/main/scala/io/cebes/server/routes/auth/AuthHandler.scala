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

package io.cebes.server.routes.auth

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import com.softwaremill.session.CsrfDirectives._
import com.softwaremill.session.CsrfOptions._
import io.cebes.auth.AuthService
import io.cebes.server.http.{SecuredSession, SessionData}
import io.cebes.server.models.CebesJsonProtocol._
import io.cebes.server.models.{OkResponse, UserLogin}
import io.cebes.server.routes.common.HandlerHelpers

/**
  * Handle all authentication requests
  */
trait AuthHandler extends SecuredSession with HandlerHelpers {

  val authService: AuthService = instance(classOf[AuthService])

  val authApi = pathPrefix("auth") {
    (path("login") & post) {
      entity(as[UserLogin]) { userLogin =>
        authService.login(userLogin.userName, userLogin.passwordHash) match {
          case true =>
            mySetSession(SessionData(userLogin.userName)) {
              setNewCsrfToken(checkHeader) { ctx =>
                ctx.complete(OkResponse("ok"))
              }
            }
          case _ =>
            throw new IllegalArgumentException("Invalid username or password")
        }
      }
    } ~ (path("logout") & post) {
      myRequiredSession { session =>
        myInvalidateSession { ctx =>
          ctx.complete(OkResponse("ok"))
        }
      }
    }
  }
}
