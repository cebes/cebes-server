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
 * Created by phvu on 09/09/16.
 */

package io.cebes.server

import spray.json.DefaultJsonProtocol

//import scala.language.implicitConversions

package object models {

  case class UserLogin(userName: String, passwordHash: String)


  /**
    * Contains all JsonProtocol for Cebes HTTP server
    */
  trait CebesJsonProtocol extends DefaultJsonProtocol {

    implicit val userLoginFormat = jsonFormat2(UserLogin)
  }

  object CebesJsonProtocol extends CebesJsonProtocol

}
