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

package io.cebes.server.routes.test

import com.google.inject.Inject
import io.cebes.df.{Dataframe, DataframeService}
import io.cebes.server.result.ResultStorage
import io.cebes.server.routes.DataframeResponse
import io.cebes.server.routes.common.AsyncSerializableOperation
import io.cebes.util.ResourceUtil

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class LoadData @Inject()(dfService: DataframeService, override val resultStorage: ResultStorage)
  extends AsyncSerializableOperation[LoadDataRequest, LoadDataResponse] {

  /**
    * Implement this to do the real work
    */
  override def runImpl(requestEntity: LoadDataRequest)
                      (implicit ec: ExecutionContext): Future[LoadDataResponse] = Future {
    LoadDataResponse(requestEntity.datasets.map {
      case "cylinder_bands" =>
        val df = LoadData.loadCylinderBands(dfService)
        DataframeResponse(df.id, df.schema.copy())
      case ds => throw new IllegalArgumentException(s"Invalid dataset name: $ds")
    })
  }


}

object LoadData {

  private val objectLocker = new Object()

  private def loadCylinderBands(dfService: DataframeService): Dataframe = {
    val resourceFile = ResourceUtil.getResourceAsFile("/data/cylinder_bands.csv")
    loadTable(dfService, "test_cylinder_bands",
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

  private def loadTable(dfService: DataframeService, tableName: String,
                        schema: String, dataFilePath: String): Dataframe = {
    // shouldn't lock on a "singleton" object, but we have no other way to make this thread-safe...
    objectLocker.synchronized {
      Try {
        val df = dfService.sql(s"SELECT * FROM $tableName")
        require(df.numRows > 10)
        df
      }.orElse {
        dfService.sql(s"CREATE TABLE IF NOT EXISTS $tableName ($schema) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
        dfService.sql(s"LOAD DATA LOCAL INPATH '$dataFilePath' INTO TABLE $tableName")
        Try(dfService.sql(s"SELECT * FROM $tableName"))
      } match {
        case Success(df) => df
        case Failure(f) => throw new IllegalArgumentException(s"Failed to load table $tableName from $dataFilePath", f)
      }
    }
  }
}
