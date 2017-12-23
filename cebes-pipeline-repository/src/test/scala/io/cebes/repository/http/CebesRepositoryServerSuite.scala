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

import java.nio.file.{Files, Path}
import java.sql.Timestamp
import java.util.Calendar
import java.util.concurrent.TimeUnit

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{HttpEntity, MediaTypes, Multipart}
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.FileIO
import io.cebes.http.client.ServerException
import io.cebes.http.helper.SecuredTestClient
import io.cebes.http.server.HttpJsonProtocol._
import io.cebes.http.server.VersionResponse
import io.cebes.repository.CebesRepositoryJsonProtocol._
import io.cebes.repository.db.Repository
import io.cebes.repository.inject.CebesRepositoryInjector
import io.cebes.repository.{RepositoryListResponse, TagListResponse, TagResponse}
import org.scalatest.FunSuite
import org.scalatest.exceptions.TestFailedException

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class CebesRepositoryServerSuite extends FunSuite with SecuredTestClient {
  private val server = CebesRepositoryInjector.instance[CebesRepositoryServer]

  override protected val serverRoutes: Route = server.routes
  override protected val apiVersion: String = CebesRepositoryServer.API_VERSION

  test("version") {
    Get("/version") ~> serverRoutes ~> check {
      val v = responseAs[VersionResponse]
      assert(v.api === CebesRepositoryServer.API_VERSION)
    }
  }

  test("full suite") {
    val ex1 = intercept[ServerException] {
      get[RepositoryListResponse]("catalog")
    }
    assert(ex1.message.startsWith("HttpResponse(308 Permanent Redirect,List(Location: catalog/0)"))

    val repos = get[RepositoryListResponse]("catalog/0")
    assert(repos.pageId === 0 && repos.totalPages >= 0)

    val ex2 = intercept[ServerException](get[Repository]("repos/abcd-def/xyz.ab/k"))
    assert(ex2.getMessage === "No such element: Repository doesn't exist: abcd-def/xyz.ab/k")

    // extra slash at the end
    val ex3 = intercept[TestFailedException](get[Repository]("repos/abcd-def/xyz.ab/k/"))
    assert(ex3.getMessage === "Request was rejected")

    val testRepoName = "phvu/test-pipeline"

    // Clean up
    try {
      delete[String, String](s"repos/$testRepoName", "")
    } catch {
      case _: ServerException =>
    }

    // list tag should fail
    val ex4 = intercept[ServerException](get[TagListResponse](s"tags/$testRepoName"))
    assert(ex4.getMessage === s"No such element: Repository doesn't exist: $testRepoName")

    // create the repo
    val r = put[String, Repository](s"repos/$testRepoName", "")
    assert(r.name === testRepoName)
    assert(r.pullCount === 0)

    // create the same repo again should fail
    val ex4a = intercept[ServerException](put[String, Repository](s"repos/$testRepoName", ""))
    assert(ex4a.message.startsWith(s"Illegal argument: Invalid repoName '$testRepoName'"))

    // catalog will contain testRepoName
    val repos2 = get[RepositoryListResponse]("catalog/0")
    assert(repos2.repositories.exists(_.name == testRepoName))

    // list tags
    val tags = get[TagListResponse](s"tags/$testRepoName")
    assert(tags.repoName === testRepoName)
    assert(tags.tags.isEmpty)

    val testTag1 = "v1"
    val testTag2 = "v2"

    // get tag info will fail
    val ex5 = intercept[ServerException](get[TagResponse](s"tag/$testTag1/$testRepoName"))
    assert(ex5.getMessage === s"No such element: Repository $testRepoName:$testTag1 not found")

    // download will fail
    val ex6 = intercept[ServerException](get(s"blob/$testTag1/$testRepoName"))
    assert(ex6.message === s"No such element: Repository $testRepoName:$testTag1 not found")

    // upload
    val ts1 = testUploadDownloadTag(testRepoName, testTag1, "Testing 1 2 3")

    // update the same tag
    val ts2 = testUploadDownloadTag(testRepoName, testTag1, "this is another test")
    assert(ts2.after(ts1))

    // list tag
    val tags1 = get[TagListResponse](s"tags/$testRepoName")
    assert(tags1.repoName === testRepoName)
    assert(tags1.tags.length === 1)
    assert(tags1.tags.exists(_.name === testTag1))

    // create a new tag
    val ts3 = testUploadDownloadTag(testRepoName, testTag2, s"this is another test for $testTag2")
    assert(ts3.after(ts2))

    // list tags
    val tags2 = get[TagListResponse](s"tags/$testRepoName")
    assert(tags2.repoName === testRepoName)
    assert(tags2.tags.length === 2)
    assert(tags2.tags.exists(_.name === testTag2))

    // clean up
    delete[String, String](s"repos/$testRepoName", "")

    val ex7 = intercept[ServerException](get[TagListResponse](s"tags/$testRepoName"))
    assert(ex7.message === s"No such element: Repository doesn't exist: $testRepoName")
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////
  // helpers

  private def createFormData(file: Path): Multipart.FormData = {
    Multipart.FormData(
      Multipart.FormData.BodyPart(
        "file",
        HttpEntity(MediaTypes.`application/octet-stream`, file.toFile.length(),
          FileIO.fromPath(file, chunkSize = 100000)),
        Map("filename" -> file.getFileName.toString)))
  }

  /**
    * Return the lastUpdate timestamp of the created tag
    */
  private def testUploadDownloadTag(repoName: String, tagName: String, fileContent: String): Timestamp = {
    val tmpFile = Files.createTempFile("cebes-ppl-repo-test-", "")

    Files.write(tmpFile, fileContent.getBytes)
    val r2 = put[Multipart.FormData, TagResponse](s"blob/$tagName/$repoName",
      createFormData(tmpFile.toAbsolutePath))
    assert(r2.name === tagName)
    assert(r2.repositoryName === Some(repoName))
    assert(r2.lastUpdate.before(new Timestamp(Calendar.getInstance().getTimeInMillis)))

    // download
    val r3 = get[HttpEntity](s"blob/$tagName/$repoName")
    val downloadedFile = Files.createTempFile("cebes-ppl-test-download-", "")
    val futureDownload = r3.dataBytes.runWith(FileIO.toPath(downloadedFile))
    Await.result(futureDownload, Duration(1, TimeUnit.MINUTES))
    assert(Files.readAllLines(downloadedFile).toArray.mkString("\n") === fileContent)

    Files.delete(tmpFile)
    Files.delete(downloadedFile)
    r2.lastUpdate
  }
}
