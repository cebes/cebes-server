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
 * Created by phvu on 23/09/16.
 */

package io.cebes.spark.helpers

import io.cebes.prop.types
import io.cebes.spark.CebesSparkTestInjector
import org.scalatest.{Ignore, Tag}

trait TestPropertyHelper {

  val properties: types.TestProperties = CebesSparkTestInjector.instance[types.TestProperties]

  object S3TestsEnabled extends Tag(if (properties.hasS3Credentials) "" else classOf[Ignore].getName)

  object JdbcTestsEnabled extends Tag(if (properties.hasJdbcCredentials) "" else classOf[Ignore].getName)
}
