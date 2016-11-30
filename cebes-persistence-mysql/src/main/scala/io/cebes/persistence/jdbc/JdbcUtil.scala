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
 * Created by phvu on 26/11/2016.
 */

package io.cebes.persistence.jdbc

import java.sql.{Connection, DriverManager, SQLException}

import com.typesafe.scalalogging.slf4j.LazyLogging

object JdbcUtil extends LazyLogging {

  private var driverLoaded = false

  private def loadDriver(driverName: String) {
    try {
      Class.forName(driverName).newInstance
      driverLoaded = true
    } catch {
      case e: Exception =>
        logger.error("Driver not available: " + e.getMessage)
        throw e
    }
  }

  def getConnection(url: String, userName: String, password: String, driver: String): Connection = {
    // Only load driver first time
    this.synchronized {
      if (!driverLoaded)
        loadDriver(driver)
    }

    try {
      DriverManager.getConnection(url, userName, password)
    } catch {
      case e: SQLException =>
        logger.error("No connection: " + e.getMessage)
        throw e
    }
  }

  /**
    * Run a task on a resource, and clean it up after that.
    * Capture [[SQLException]], if it happens.
    */
  def cleanJdbcCall[A, B](resource: A)(cleanup: A => Unit)(doWork: A => B): B =
    try {
      doWork(resource)
    } catch {
      case e: SQLException =>
        logger.error("Failed when running the job on resource", e)
        throw e
    }
    finally {
      try {
        if (resource != null) {
          cleanup(resource)
        }
      } catch {
        case e: SQLException =>
          logger.error("Failed to clean up resource", e)
      }
    }
}
