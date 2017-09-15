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

package io.cebes.http.server.auth

import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

case class LoginRequest(userName: String, passwordHash: String)

case class LoginResponse(message: String)

trait HttpAuthJsonProtocol {

  implicit val loginRequestFormat: RootJsonFormat[LoginRequest] = jsonFormat2(LoginRequest)
  implicit val loginResponseFormat: RootJsonFormat[LoginResponse] = jsonFormat1(LoginResponse)

}

object HttpAuthJsonProtocol extends HttpAuthJsonProtocol
