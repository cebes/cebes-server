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
 * Created by phvu on 29/09/16.
 */

package io.cebes.util

import java.io.{File, IOException}
import java.nio.file.{Files, Paths}

trait ResourceUtil {

  /**
    * Get resource as a file
    *
    * @param resourceName should contain a "/" at the beginning
    * @return
    */
  def getResourceAsFile(resourceName: String): File = {
    val file = Option(getClass.getResource(resourceName)) match {
      case Some(url) if url.toString.startsWith("jar:") =>
        try {
          val inputStream = getClass.getResourceAsStream(resourceName)
          val tmpFile = File.createTempFile("tempfile", ".tmp")
          Files.copy(inputStream, Paths.get(tmpFile.getPath))
          Some(tmpFile)
        } catch {
          case ex: IOException =>
            None
        }
      case Some(url) => Some(new File(url.getFile))
      case None => None
    }

    file match {
      case Some(f) if f.exists() => f
      case _ => throw new RuntimeException("File " + resourceName + " not found!")
    }
  }
}

object ResourceUtil extends ResourceUtil