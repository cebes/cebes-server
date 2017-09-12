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

package io.cebes.http.server.routes

import java.io.{PrintWriter, StringWriter}

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.ExceptionHandler
import io.cebes.http.server.FailResponse
import io.cebes.http.server.HttpJsonProtocol._

import scala.concurrent.{ExecutionContext, Future}

trait ApiErrorHandler extends AkkaImplicits {

  implicit def myExceptionHandler: ExceptionHandler = ExceptionHandler {
    case e: NoSuchElementException =>
      complete(ApiErrorHandler.getHttpResponse(NotFound, s"No such element: ${e.getMessage}", e))
    case e: IllegalArgumentException =>
      complete(ApiErrorHandler.getHttpResponse(BadRequest, s"Illegal argument: ${e.getMessage}", e))
    case e =>
      complete(ApiErrorHandler.getHttpResponse(InternalServerError, e.getMessage, e))
  }
}

object ApiErrorHandler {

  private def getHttpResponse(statusCode: StatusCode, message: String, ex: Throwable)
                             (implicit ec: ExecutionContext): Future[HttpResponse] = {
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)
    ex.printStackTrace(pw)

    Marshal(FailResponse(Option(message), Option(sw.toString))).to[ResponseEntity].map { entity =>
      HttpResponse(statusCode, entity = entity.withContentType(ContentTypes.`application/json`))
    }
  }
}
