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

package io.cebes.server.helpers

import io.cebes.client.RemoteDataframe
import io.cebes.util.ResourceUtil

/**
  * Helper trait for all the test, containing the services
  */
trait TestDataHelper {

  def sendSql(sqlText: String): RemoteDataframe

  def createOrReplaceHiveTable(tableName: String, schema: String, dataFilePath: String): RemoteDataframe = {
    sendSql(s"DROP TABLE IF EXISTS $tableName")
    sendSql(s"CREATE TABLE $tableName ($schema) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    sendSql(s"LOAD DATA LOCAL INPATH '$dataFilePath' INTO TABLE $tableName")
  }

  val cylinderBandsTableName = s"cylinder_bands_${getClass.getCanonicalName.replace(".", "_").toLowerCase}"

  def getCylinderBands: RemoteDataframe = sendSql(s"SELECT * FROM $cylinderBandsTableName")

  def createOrReplaceCylinderBands(tableName: Option[String] = None): RemoteDataframe = {
    val resourceFile = ResourceUtil.getResourceAsFile("/data/cylinder_bands.csv")
    createOrReplaceHiveTable(tableName.getOrElse(cylinderBandsTableName),
      "timestamp LONG, cylinder_number STRING, " +
        "customer STRING, job_number INT, grain_screened STRING, ink_color STRING, " +
        "proof_on_ctd_ink STRING, blade_mfg STRING, cylinder_division STRING, paper_type STRING, " +
        "ink_type STRING, direct_steam STRING, solvent_type STRING, type_on_cylinder STRING, " +
        "press_type STRING, press INT, unit_number INT, cylinder_size STRING, paper_mill_location STRING, " +
        "plating_tank INT, proof_cut FLOAT, viscosity INT, caliper FLOAT, ink_temperature FLOAT, " +
        "humifity INT, roughness FLOAT, blade_pressure INT, varnish_pct FLOAT, press_speed FLOAT, " +
        "ink_pct FLOAT, solvent_pct FLOAT, esa_voltage FLOAT, esa_amperage FLOAT, wax FLOAT, " +
        "hardener FLOAT, roller_durometer INT, current_density INT, anode_space_ratio FLOAT, " +
        "chrome_content FLOAT, band_type STRING", resourceFile.toString)
  }
}
