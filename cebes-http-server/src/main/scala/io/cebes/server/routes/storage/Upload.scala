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
 * Created by phvu on 17/09/16.
 */

package io.cebes.server.routes.storage

import akka.http.scaladsl.model.Multipart.FormData
import akka.stream.Materializer
import akka.util.ByteString
import io.cebes.server.inject.InjectorService
import io.cebes.server.models.UploadResponse
import io.cebes.storage.DataWriter

import scala.concurrent.{ExecutionContext, Future}

class Upload {

  val dataWriter = InjectorService.injector.getInstance(classOf[DataWriter])

  /**
    * Implement this to do the real work
    */
  def run(requestEntity: FormData)
         (implicit ec: ExecutionContext,
          materializer: Materializer): Future[UploadResponse] = {

    val v = requestEntity.parts.mapAsync(1) { bodyPart =>
      def writeFile(array: Array[Byte], byteString: ByteString): Array[Byte] = {
        val byteArray: Array[Byte] = byteString.toArray
        dataWriter.append(byteArray)
        array ++ byteArray
      }
      bodyPart.entity.dataBytes.runFold(Array[Byte]())(writeFile)
    }.runFold(0)(_ + _.length)
    v.map { s =>
      val p = dataWriter.path
      dataWriter.close()
      UploadResponse(p, s)
    }
  }
}
