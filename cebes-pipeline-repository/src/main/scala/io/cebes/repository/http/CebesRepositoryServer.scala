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
package io.cebes.repository.http

import java.nio.file.Files

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{ContentTypes, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import com.google.inject.Inject
import io.cebes.auth.AuthService
import io.cebes.http.server.HttpServer
import io.cebes.http.server.auth.AuthHandler
import io.cebes.http.server.routes.ApiErrorHandler
import io.cebes.persistence.jdbc.JdbcUtil
import io.cebes.prop.types.MySqlBackendCredentials
import io.cebes.prop.{Prop, Property}
import io.cebes.repository.CebesRepositoryJsonProtocol._
import io.cebes.repository.db.RepositoryDatabase
import io.cebes.repository.inject.CebesRepositoryInjector
import io.cebes.repository.{PipelineRepositoryService, VersionResponse}
import org.squeryl.adapters.MySQLAdapter
import org.squeryl.{Session, SessionFactory}

import scala.concurrent.ExecutionContextExecutor


class CebesRepositoryServer @Inject()(@Prop(Property.REPOSITORY_INTERFACE) override val httpInterface: String,
                                      @Prop(Property.REPOSITORY_PORT) override val httpPort: Int,
                                      override protected val refreshTokenStorage: CebesRepositoryRefreshTokenStorage,
                                      override protected val authService: AuthService,
                                      private val mysqlCreds: MySqlBackendCredentials)
  extends HttpServer with AuthHandler with ApiErrorHandler {

  override protected val serverSecret: String =
    "v8Km83QULVYHVgx0GxJKkZ7v3uhtA3wVY3maYArW5fI1WFTpUwyXQQLwGjVfirAA5OuIVv"

  protected implicit val actorSystem: ActorSystem = ActorSystem("CebesPipelineRepository")
  protected implicit val actorExecutor: ExecutionContextExecutor = actorSystem.dispatcher
  protected implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()

  CebesRepositoryServer.initializeDb(mysqlCreds)

  val repoRoutes: Route = requiredCebesSession { _ =>
    pathPrefix("catalog") {
      // listing repository names (i.e. repositories)
      (path(PathEnd) & get) {
        redirect("catalog/0", StatusCodes.PermanentRedirect)

      } ~ (path(LongNumber) & get) { pageId =>
        complete(repoService.listRepositories(Some(pageId)))
      }

    } ~ pathPrefix("repos" / Segment) { repoName =>
      path(PathEnd) {
        put {
          // create a new repo
          complete(repoService.createRepository(repoName, isPrivate = false))
        } ~ get {
          // get repository information
          complete(repoService.getRepositoryInfo(repoName))
        }

      } ~ pathPrefix("tags") {

        // list all tags of this pipeline
        (path(PathEnd) & get) {
          complete(repoService.listTags(repoName))

        } ~ pathPrefix(Segment) { tagName =>

          (path("download") & get) {
            // raise exception if the tag doesn't exist
            repoService.getTagInfo(repoName, tagName)
            getFromFile(repoService.getPath(repoName, tagName).toFile, ContentTypes.`application/octet-stream`)

          } ~ (path("upload") & put) {
            // make sure the repo exist: raise exception if it doesn't
            repoService.getRepositoryInfo(repoName)

            // process the binary file, add a new tag
            uploadedFile("file") { case (_, file) =>
              Files.move(file.toPath, repoService.getPath(repoName, tagName))
              complete(repoService.insertOrUpdateTag(repoName, tagName))
            }

          } ~ (path("info") & get) {
            complete(repoService.getTagInfo(repoName, tagName))
          }
        }
      }
    }
  }

  override val routes: Route = handleExceptions(cebesDefaultExceptionHandler) {
    pathPrefix(CebesRepositoryServer.API_VERSION) {
      authApi ~ repoRoutes
    } ~ (path("version") & get) {
      complete(VersionResponse(CebesRepositoryServer.API_VERSION))
    }
  }

  private def repoService: PipelineRepositoryService = CebesRepositoryInjector.instance[PipelineRepositoryService]

  override def start(): Unit = {
    super.start()
  }
}

object CebesRepositoryServer {
  val API_VERSION = "v1"

  private def initializeDb(mysqlCreds: MySqlBackendCredentials): Unit = {
    // initialize Squeryl sessions
    SessionFactory.concreteFactory = Some(() =>
      Session.create(JdbcUtil.getConnection(mysqlCreds.url, mysqlCreds.userName,
        mysqlCreds.password, mysqlCreds.driver),
        new MySQLAdapter))

    RepositoryDatabase.initialize()
  }
}
