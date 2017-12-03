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

import java.nio.file.{Files, Paths}

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.FileIO
import com.google.inject.Inject
import io.cebes.http.client.Client
import io.cebes.pipeline.factory.PipelineFactory
import io.cebes.pipeline.json.PipelineExportDef
import io.cebes.pipeline.models.Pipeline
import io.cebes.tag.Tag
import io.cebes.util.FileSystemHelper
import spray.json.JsonFormat

import scala.concurrent.{ExecutionContext, Future}

/**
  * Client of Pipeline repository
  */
class RepositoryClient @Inject()(pplFactory: PipelineFactory) {

  def download(repoTag: Tag, userName: Option[String], passwordHash: Option[String])
              (implicit jsPplEx: JsonFormat[PipelineExportDef],
               actorSystem: ActorSystem, actorMaterializer: ActorMaterializer,
               ec: ExecutionContext): Future[Pipeline] = {
    getClient(repoTag.host, repoTag.port, userName, passwordHash).flatMap { client =>

      client.requestAsync[String, HttpEntity](
        HttpMethods.GET, s"blob/${repoTag.version}/${repoTag.path}", "").flatMap { r =>

        val downloadedFile = Files.createTempFile("cebes-ppl-download-", "")
        r.dataBytes.runWith(FileIO.toPath(downloadedFile)).map(_ => downloadedFile)

      }.map { downloadedFile =>
        pplFactory.importZip(downloadedFile.toString)
      }
    }
  }


  def upload(ppl: Pipeline, repoTag: Tag, userName: Option[String], passwordHash: Option[String])
            (implicit jsPplEx: JsonFormat[PipelineExportDef],
             actorSystem: ActorSystem, actorMaterializer: ActorMaterializer,
             ec: ExecutionContext): Future[String] = {
    pplFactory.exportZip(ppl, Files.createTempFile("cebes-ppl-upload-", "").toString).flatMap { filePath =>
      getClient(repoTag.host, repoTag.port, userName, passwordHash).flatMap { client =>
        val fp = Paths.get(filePath).toAbsolutePath
        val formData = Multipart.FormData(
          Multipart.FormData.BodyPart(
            "file",
            HttpEntity(MediaTypes.`application/octet-stream`, fp.toFile.length(),
              FileIO.fromPath(fp, chunkSize = 100000)),
            Map("filename" -> fp.getFileName.toString)))

        val uploadResult = client.requestAsync[Multipart.FormData, String](
          HttpMethods.PUT, s"blob/${repoTag.version}/${repoTag.path}", formData)

        uploadResult.onComplete { _ =>
          FileSystemHelper.deleteRecursively(fp.toFile, silent = true)
        }
        uploadResult
      }
    }

  }


  private def getClient(host: Option[String], port: Option[Int], userName: Option[String], passwordHash: Option[String])
                       (implicit actorSystem: ActorSystem, actorMaterializer: ActorMaterializer,
                        ec: ExecutionContext): Future[Client] = {
    val client = new Client(host.getOrElse(RepositoryClient.DEFAULT_HOST),
      port.getOrElse(RepositoryClient.DEFAULT_PORT))
    if (userName.isEmpty && passwordHash.isEmpty) {
      Future.successful(client)
    } else {
      client.login(userName.getOrElse(""), passwordHash.getOrElse("")).map(_ => client)
    }
  }
}

object RepositoryClient {
  private val DEFAULT_HOST = "localhost"
  private val DEFAULT_PORT = 80
}
