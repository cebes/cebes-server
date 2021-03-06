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
package io.cebes.serving.http

import io.cebes.http.helper.SecuredTestClient
import io.cebes.http.server.HttpServer
import io.cebes.serving.inject.ServingTestInjector

/**
  * Tests for [[CebesServingSecuredServer]].
  * All the tests are inherited from [[CebesServingServerSuite]], but with secured server.
  */
class CebesServingSecuredServerSuite extends CebesServingServerSuite with SecuredTestClient {

  override protected val server: HttpServer =
    ServingTestInjector.injector.getInstance(classOf[CebesServingSecuredServer])
}
