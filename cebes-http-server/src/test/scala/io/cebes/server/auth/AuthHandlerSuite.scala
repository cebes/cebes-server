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
 * Created by phvu on 07/09/16.
 */

package io.cebes.server.auth


import com.typesafe.scalalogging.slf4j.StrictLogging
import io.cebes.server.helpers.HasClient


class AuthHandlerSuite extends HasClient with StrictLogging {

  test("login") {
    Thread.sleep(1000)
    logger.info("In AuthHandlerSuite")
    assert((3 + 1) !== 2)
  }
}
