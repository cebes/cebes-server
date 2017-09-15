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
package io.cebes.http.server.routes

import akka.http.scaladsl.server.{Directive0, Directive1}
import com.softwaremill.session.CsrfDirectives.{randomTokenCsrfProtection, setNewCsrfToken}
import com.softwaremill.session.CsrfOptions.checkHeader
import com.softwaremill.session.SessionDirectives.{invalidateSession, requiredSession, setSession}
import com.softwaremill.session.SessionOptions.{refreshable, usingHeaders}
import com.softwaremill.session.{BasicSessionEncoder, RefreshTokenStorage, SessionConfig, SessionManager}


trait SecuredSession extends AkkaImplicits {

  //////////////////////////////////////////////////////////////////////////////
  // to be overridden
  protected implicit def refreshTokenStorage: RefreshTokenStorage[SessionData]

  protected val serverSecret: String

  // for http-session
  private lazy val sessionConfig: SessionConfig = SessionConfig.default(serverSecret)

  private implicit lazy val encoder: BasicSessionEncoder[SessionData] = new BasicSessionEncoder[SessionData]

  private implicit lazy val sessionManager: SessionManager[SessionData] = new SessionManager[SessionData](sessionConfig)


  protected def setCebesSession(v: SessionData): Directive0 = setSession(refreshable, usingHeaders, v)

  protected def setCebesCsrfToken(): Directive0 =  setNewCsrfToken(checkHeader)

  protected lazy val requiredCebesSession: Directive1[SessionData] =
    randomTokenCsrfProtection(checkHeader) & requiredSession(refreshable, usingHeaders)

  protected lazy val invalidateCebesSession: Directive0 = invalidateSession(refreshable, usingHeaders)
}
