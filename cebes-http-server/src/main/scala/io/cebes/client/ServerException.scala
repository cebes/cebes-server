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
 * Created by phvu on 21/09/16.
 */

package io.cebes.client

import java.util.UUID

case class ServerException(requestId: Option[UUID],
                           message: String,
                           serverStacktrace: Option[String]) extends Exception(message) {

  override def toString: String = {
    val s = getClass.getName
    val result = Option(getLocalizedMessage) match {
      case Some(msg) => s"$s: $msg"
      case None => s
    }
    serverStacktrace match {
      case Some(str) => s"$result\nServer Stacktrace: $str"
      case None => result
    }
  }
}
