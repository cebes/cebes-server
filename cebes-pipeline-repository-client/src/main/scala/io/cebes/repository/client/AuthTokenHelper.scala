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
package io.cebes.repository.client

import java.nio.charset.StandardCharsets
import java.util.Base64

import akka.http.scaladsl.model.HttpHeader
import io.cebes.repository.client.HttpHeaderJsonProtocol._
import spray.json.DefaultJsonProtocol.seqFormat
import spray.json._

trait AuthTokenHelper {

  /**
    * Encode the given sequence of HttpHeaders into a string
    * (to be sent to clients)
    */
  def encode(headers: Seq[HttpHeader]): String = {
    Base64.getUrlEncoder.encodeToString(headers.toJson.toString().getBytes(StandardCharsets.UTF_8))
  }

  /**
    * Decode the given authentication token into a sequence of HttpHeader
    */
  def decode(authToken: String): Seq[HttpHeader] = {
    val js = new String(Base64.getUrlDecoder.decode(authToken), StandardCharsets.UTF_8)
    js.parseJson.convertTo[Seq[HttpHeader]]
  }
}
