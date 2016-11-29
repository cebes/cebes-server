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

package io.cebes.server.http

import akka.http.scaladsl.server.{Directive0, Directive1}
import com.softwaremill.session.CsrfDirectives._
import com.softwaremill.session.CsrfOptions._
import com.softwaremill.session.SessionDirectives._
import com.softwaremill.session.SessionOptions._
import com.softwaremill.session._
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.cebes.server.inject.InjectorService

import scala.concurrent.ExecutionContext.Implicits.global

trait SecuredSession extends StrictLogging {

  val sessionConfig: SessionConfig =
    SessionConfig.default("9MLs9gc8Axvdi1tbM1T7ZpjFMM5R5QR7b788MAIdlloi5I8FmXNQuTdn9S3hnlcZPmC0sv0")

  implicit val encoder = new BasicSessionEncoder[SessionData]
  implicit val sessionManager = new SessionManager[SessionData](sessionConfig)

  implicit val refreshTokenStorage: RefreshTokenStorage[SessionData] =
    InjectorService.instance(classOf[JdbcRefreshTokenStorage])

  def mySetSession(v: SessionData): Directive0 = setSession(refreshable, usingHeaders, v)

  val myRequiredSession: Directive1[_] =
    randomTokenCsrfProtection(checkHeader) & requiredSession(refreshable, usingHeaders)
  val myInvalidateSession: Directive0 = invalidateSession(refreshable, usingHeaders)
}
