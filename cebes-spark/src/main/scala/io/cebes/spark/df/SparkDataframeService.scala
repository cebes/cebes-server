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
 * Created by phvu on 25/08/16.
 */

package io.cebes.spark.df

import java.io.{BufferedReader, IOException, InputStreamReader}
import java.util.UUID

import com.google.inject.Inject
import com.typesafe.scalalogging.LazyLogging
import io.cebes.df.DataframeService.AggregationTypes
import io.cebes.df.sample.DataSample
import io.cebes.df.schema.Schema
import io.cebes.df.support.GroupedDataframe
import io.cebes.df.types.VariableTypes.VariableType
import io.cebes.df.{Column, Dataframe, DataframeService}
import io.cebes.json.CebesCoreJsonProtocol._
import io.cebes.spark.config.HasSparkSession
import io.cebes.spark.util.CebesSparkUtil
import io.cebes.store.{CachedStore, TagStore}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.deploy.SparkHadoopUtil
import spray.json._

/**
  * Implements [[DataframeService]] on Spark.
  *
  * This class can be instantiated multiple times from the DI framework
  */
class SparkDataframeService @Inject()(hasSparkSession: HasSparkSession,
                                      dfFactory: SparkDataframeFactory,
                                      override val cachedStore: CachedStore[Dataframe],
                                      override val tagStore: TagStore[Dataframe])
  extends DataframeService with LazyLogging {

  override def cache(df: Dataframe): Dataframe = cachedStore.add(df)

  override def sql(sqlText: String): Dataframe = cache {
    dfFactory.df(hasSparkSession.session.sql(sqlText))
  }

  override def inferVariableTypes(dfId: UUID, sampleSize: Int): Dataframe = cache {
    cachedStore(dfId).inferVariableTypes(sampleSize)
  }

  override def withVariableTypes(dfId: UUID, variableTypes: Map[String, VariableType]): Dataframe = cache {
    cachedStore(dfId).withVariableTypes(variableTypes)
  }

  override def count(dfId: UUID): Long = cachedStore(dfId).count()

  override def take(dfId: UUID, n: Int): DataSample = {
    cachedStore(dfId).take(n)
  }

  override def sample(dfId: UUID, withReplacement: Boolean,
                      fraction: Double, seed: Long): Dataframe = cache {
    cachedStore(dfId).sample(withReplacement, fraction, seed)
  }

  ////////////////////////////////////////////////////////////////////////////////////
  // Data exploration
  ////////////////////////////////////////////////////////////////////////////////////

  override def sort(dfId: UUID, sortExprs: Column*): Dataframe = cache {
    cachedStore(dfId).sort(sortExprs: _*)
  }

  override def drop(dfId: UUID, colNames: Seq[String]): Dataframe = cache {
    cachedStore(dfId).drop(colNames)
  }

  override def dropDuplicates(dfId: UUID, colNames: Seq[String]): Dataframe = cache {
    cachedStore(dfId).dropDuplicates(colNames)
  }

  override def dropNA(dfId: UUID, minNonNulls: Int, cols: Seq[String]): Dataframe = cache {
    cachedStore(dfId).na.drop(minNonNulls, cols)
  }

  override def fillNA(dfId: UUID, value: Either[String, Double], cols: Seq[String]): Dataframe = cache {
    val df = cachedStore(dfId)
    value match {
      case Right(d) => df.na.fill(d, cols)
      case Left(s) => df.na.fill(s, cols)
    }
  }

  override def fillNA(dfId: UUID, valueMap: Map[String, Any]): Dataframe = cache {
    cachedStore(dfId).na.fill(valueMap)
  }

  override def replace[T](dfId: UUID, cols: Seq[String], replacement: Map[T, T]): Dataframe = cache {
    cachedStore(dfId).na.replace[T](cols, replacement)
  }

  override def approxQuantile(dfId: UUID, col: String, probabilities: Array[Double],
                              relativeError: Double): Array[Double] = {
    cachedStore(dfId).stat.approxQuantile(col, probabilities, relativeError)
  }

  override def cov(dfId: UUID, col1: String, col2: String): Double = {
    cachedStore(dfId).stat.cov(col1, col2)
  }

  override def corr(dfId: UUID, col1: String, col2: String): Double = {
    cachedStore(dfId).stat.corr(col1, col2)
  }

  override def crosstab(dfId: UUID, col1: String, col2: String): Dataframe = cache {
    cachedStore(dfId).stat.crosstab(col1, col2)
  }

  override def freqItems(dfId: UUID, cols: Seq[String], support: Double): Dataframe = cache {
    cachedStore(dfId).stat.freqItems(cols, support)
  }

  override def sampleBy[T](dfId: UUID, col: String, fractions: Map[T, Double], seed: Long): Dataframe = cache {
    cachedStore(dfId).stat.sampleBy(col, fractions, seed)
  }

  ////////////////////////////////////////////////////////////////////////////////////
  // SQL-related functions
  ////////////////////////////////////////////////////////////////////////////////////

  /**
    * Returns a new [[Dataframe]] by adding a column or replacing
    * the existing column that has the same name (case-insensitive).
    */
  override def withColumn(dfId: UUID, colName: String, col: Column): Dataframe = cache {
    cachedStore(dfId).withColumn(colName, col)
  }

  /**
    * Returns a new [[Dataframe]] with a column renamed.
    */
  override def withColumnRenamed(dfId: UUID, existingName: String, newName: String): Dataframe = cache {
    cachedStore(dfId).withColumnRenamed(existingName, newName)
  }

  /**
    * Selects a set of columns based on expressions.
    */
  override def select(dfId: UUID, columns: Column*): Dataframe = cache {
    cachedStore(dfId).select(columns: _*)
  }

  /**
    * Filters rows using the given condition.
    */
  override def where(dfId: UUID, column: Column): Dataframe = cache {
    cachedStore(dfId).where(column)
  }

  /**
    * Returns a new Dataframe with an alias set.
    */
  override def alias(dfId: UUID, alias: String): Dataframe = cache {
    cachedStore(dfId).alias(alias)
  }

  /**
    * Join with another [[Dataframe]], using the given join expression.
    */
  override def join(leftDfId: UUID, rightDfId: UUID, joinExprs: Column, joinType: String): Dataframe = cache {
    cachedStore(leftDfId).join(cachedStore(rightDfId), joinExprs, joinType)
  }

  /**
    * Returns a new [[Dataframe]] by taking the first `n` rows.
    */
  override def limit(dfId: UUID, n: Int): Dataframe = cache {
    cachedStore(dfId).limit(n)
  }

  /**
    * Returns a new Dataframe containing union of rows in this Dataframe and another Dataframe
    * (without deduplication)
    *
    * @group sql-api
    */
  override def union(dfId: UUID, otherDfId: UUID): Dataframe = cache {
    cachedStore(dfId).union(cachedStore(otherDfId))
  }

  /**
    * Returns a new Dataframe containing rows only in both this Dataframe and another Dataframe.
    *
    * Note that, equality checking is performed directly on the encoded representation of the data
    * and thus is not affected by a custom `equals` function.
    *
    * @group sql-api
    */
  override def intersect(dfId: UUID, otherDfId: UUID): Dataframe = cache {
    cachedStore(dfId).intersect(cachedStore(otherDfId))
  }

  /**
    * Returns a new Dataframe containing rows in this Dataframe but not in another Dataframe.
    * This is equivalent to `EXCEPT` in SQL.
    *
    * Note that, equality checking is performed directly on the encoded representation of the data
    * and thus is not affected by a custom `equals` function.
    *
    * @group sql-api
    */
  override def except(dfId: UUID, otherDfId: UUID): Dataframe = cache {
    cachedStore(dfId).except(cachedStore(otherDfId))
  }

  /**
    * Marks a DataFrame as small enough for use in broadcast joins.
    *
    * @group sql-api
    */
  override def broadcast(dfId: UUID): Dataframe = cache {
    cachedStore(dfId).broadcast
  }

  ////////////////////////////////////////////////////////////////////////////////////
  // Aggregation functions
  ////////////////////////////////////////////////////////////////////////////////////

  override def aggregateAgg(dfId: UUID, cols: Seq[Column], aggType: DataframeService.AggregationTypes.AggregationType,
                            pivotColName: Option[String], pivotValues: Option[Seq[Any]],
                            aggExprs: Seq[Column]): Dataframe = cache {
    agg(dfId, cols, aggType, pivotColName, pivotValues).agg(aggExprs.head, aggExprs.tail: _*)
  }

  override def aggregateCount(dfId: UUID, cols: Seq[Column], aggType: DataframeService.AggregationTypes.AggregationType,
                              pivotColName: Option[String], pivotValues: Option[Seq[Any]]): Dataframe = cache {
    agg(dfId, cols, aggType, pivotColName, pivotValues).count()
  }

  override def aggregateMin(dfId: UUID, cols: Seq[Column], aggType: DataframeService.AggregationTypes.AggregationType,
                            pivotColName: Option[String], pivotValues: Option[Seq[Any]],
                            minColNames: Seq[String]): Dataframe = cache {
    agg(dfId, cols, aggType, pivotColName, pivotValues).min(minColNames: _*)
  }

  override def aggregateMean(dfId: UUID, cols: Seq[Column], aggType: DataframeService.AggregationTypes.AggregationType,
                             pivotColName: Option[String], pivotValues: Option[Seq[Any]],
                             meanColNames: Seq[String]): Dataframe = cache {
    agg(dfId, cols, aggType, pivotColName, pivotValues).mean(meanColNames: _*)
  }

  override def aggregateMax(dfId: UUID, cols: Seq[Column], aggType: DataframeService.AggregationTypes.AggregationType,
                            pivotColName: Option[String], pivotValues: Option[Seq[Any]],
                            maxColNames: Seq[String]): Dataframe = cache {
    agg(dfId, cols, aggType, pivotColName, pivotValues).max(maxColNames: _*)
  }

  override def aggregateSum(dfId: UUID, cols: Seq[Column], aggType: DataframeService.AggregationTypes.AggregationType,
                            pivotColName: Option[String], pivotValues: Option[Seq[Any]],
                            sumColNames: Seq[String]): Dataframe = cache {
    agg(dfId, cols, aggType, pivotColName, pivotValues).sum(sumColNames: _*)
  }

  ////////////////////////////////////////////////////////
  // JSON serialization
  ////////////////////////////////////////////////////////

  override def serialize(df: Dataframe): JsValue = {
    val hdfsConf = SparkHadoopUtil.get.newConfiguration(hasSparkSession.session.sparkContext.getConf)
    val hdfs = FileSystem.get(hdfsConf)

    val pathTemplate = s"/tmp/${df.id.toString}"

    def checkExist(i: Int): String = {
      val path = if (i == 0) pathTemplate else s"${pathTemplate}_$i"
      if (!hdfs.exists(new Path(path))) {
        path
      } else {
        checkExist(i + 1)
      }
    }

    val outDir = checkExist(0)

    // write the Dataframe into outDir in JSON
    CebesSparkUtil.getSparkDataframe(df).sparkDf.write.json(outDir)

    // read the JSON from outDir
    val data = hdfs.globStatus(new Path(outDir + "/*.json")).flatMap { fileStatus =>
      val f = hdfs.open(fileStatus.getPath)
      val reader = new BufferedReader(new InputStreamReader(f))

      def readLines = Stream.cons(reader.readLine, Stream.continually(reader.readLine))

      readLines.takeWhile(_ != null).map(_.parseJson)
    }
    val jsObject = JsObject(Map("schema" -> df.schema.toJson, "data" -> JsArray(data.toVector)))
    try {
      hdfs.delete(new Path(outDir), true)
    } catch {
      case ex: IOException =>
        logger.error(s"Failed to remove temporary directory $outDir", ex)
    }
    jsObject
  }

  /**
    * Deserialize the given JSON value into a [[Dataframe]]
    * To go with [[serialize]]
    */
  def deserialize(jsValue: JsValue): Dataframe = {
    jsValue match {
      case jsObj: JsObject =>
        require(jsObj.fields.contains("data"), "JsObject must contain data field")

        val sparkDf = jsObj.fields("data") match {
          case jsArr: JsArray =>
            import hasSparkSession.session.implicits._

            val df1 = hasSparkSession.session.createDataset(jsArr.elements.map(_.toString()))
            hasSparkSession.session.read.json(df1)
          case _ =>
            throw new IllegalArgumentException("data must be an array of objects")
        }

        jsObj.fields.get("schema").map(_.convertTo[Schema]) match {
          case Some(schema) => dfFactory.df(sparkDf, schema)
          case None => dfFactory.df(sparkDf)
        }
      case _ =>
        throw new IllegalArgumentException("Cannot deserialize the given JSON into Dataframe")
    }
  }

  ////////////////////////
  // aggregation helper
  ////////////////////////

  private def agg(dfId: UUID, cols: Seq[Column], aggType: DataframeService.AggregationTypes.AggregationType,
                  pivotColName: Option[String], pivotValues: Option[Seq[Any]]): GroupedDataframe = {
    val gdf1 = aggType match {
      case AggregationTypes.GroupBy =>
        cachedStore(dfId).groupBy(cols: _*)
      case AggregationTypes.RollUp =>
        cachedStore(dfId).rollup(cols: _*)
      case AggregationTypes.Cube =>
        cachedStore(dfId).cube(cols: _*)
    }
    pivotColName.fold(gdf1) { colName =>
      pivotValues.fold(gdf1.pivot(colName)) { values =>
        gdf1.pivot(colName, values)
      }
    }
  }
}
