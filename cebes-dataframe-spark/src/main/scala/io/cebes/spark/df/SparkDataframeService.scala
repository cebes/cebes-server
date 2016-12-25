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

import java.util.UUID

import com.google.inject.Inject
import io.cebes.df.DataframeService.AggregationTypes
import io.cebes.df.sample.DataSample
import io.cebes.df.support.GroupedDataframe
import io.cebes.df.{Column, Dataframe, DataframeService, DataframeStore}
import io.cebes.spark.config.HasSparkSession

/**
  * Implements [[DataframeService]] on Spark.
  *
  * This class can be instantiated multiple times from the DI framework
  */
class SparkDataframeService @Inject()(hasSparkSession: HasSparkSession,
                                      dfStore: DataframeStore,
                                      dfFactory: SparkDataframeFactory) extends DataframeService {

  private val sparkSession = hasSparkSession.session


  override def sql(sqlText: String): Dataframe = dfStore.add {
    dfFactory.df(sparkSession.sql(sqlText))
  }

  override def count(dfId: UUID): Long = dfStore(dfId).count()

  override def take(dfId: UUID, n: Int): DataSample = {
    dfStore(dfId).take(n)
  }

  override def sample(dfId: UUID, withReplacement: Boolean,
                      fraction: Double, seed: Long): Dataframe = dfStore.add {
    dfStore(dfId).sample(withReplacement, fraction, seed)
  }

  ////////////////////////////////////////////////////////////////////////////////////
  // Data exploration
  ////////////////////////////////////////////////////////////////////////////////////

  override def sort(dfId: UUID, sortExprs: Column*): Dataframe = dfStore.add {
    dfStore(dfId).sort(sortExprs: _*)
  }

  override def drop(dfId: UUID, colNames: Seq[String]): Dataframe = dfStore.add {
    dfStore(dfId).drop(colNames)
  }

  override def dropDuplicates(dfId: UUID, colNames: Seq[String]): Dataframe = dfStore.add {
    dfStore(dfId).dropDuplicates(colNames)
  }

  override def dropNA(dfId: UUID, minNonNulls: Int, cols: Seq[String]): Dataframe = dfStore.add {
    dfStore(dfId).na.drop(minNonNulls, cols)
  }

  override def fillNA(dfId: UUID, value: Either[String, Double], cols: Seq[String]): Dataframe = dfStore.add {
    val df = dfStore(dfId)
    value match {
      case Right(d) => df.na.fill(d, cols)
      case Left(s) => df.na.fill(s, cols)
    }
  }

  override def fillNA(dfId: UUID, valueMap: Map[String, Any]): Dataframe = dfStore.add {
    dfStore(dfId).na.fill(valueMap)
  }

  override def replace[T](dfId: UUID, cols: Seq[String], replacement: Map[T, T]): Dataframe = dfStore.add {
    dfStore(dfId).na.replace[T](cols, replacement)
  }

  override def approxQuantile(dfId: UUID, col: String, probabilities: Array[Double],
                              relativeError: Double): Array[Double] = {
    dfStore(dfId).stat.approxQuantile(col, probabilities, relativeError)
  }

  override def cov(dfId: UUID, col1: String, col2: String): Double = {
    dfStore(dfId).stat.cov(col1, col2)
  }

  override def corr(dfId: UUID, col1: String, col2: String): Double = {
    dfStore(dfId).stat.corr(col1, col2)
  }

  override def crosstab(dfId: UUID, col1: String, col2: String): Dataframe = dfStore.add {
    dfStore(dfId).stat.crosstab(col1, col2)
  }

  override def freqItems(dfId: UUID, cols: Seq[String], support: Double): Dataframe = dfStore.add {
    dfStore(dfId).stat.freqItems(cols, support)
  }

  override def sampleBy[T](dfId: UUID, col: String, fractions: Map[T, Double], seed: Long): Dataframe = dfStore.add {
    dfStore(dfId).stat.sampleBy(col, fractions, seed)
  }

  ////////////////////////////////////////////////////////////////////////////////////
  // SQL-related functions
  ////////////////////////////////////////////////////////////////////////////////////

  /**
    * Returns a new [[Dataframe]] by adding a column or replacing
    * the existing column that has the same name (case-insensitive).
    */
  override def withColumn(dfId: UUID, colName: String, col: Column): Dataframe = dfStore.add {
    dfStore(dfId).withColumn(colName, col)
  }

  /**
    * Returns a new [[Dataframe]] with a column renamed.
    */
  override def withColumnRenamed(dfId: UUID, existingName: String, newName: String): Dataframe = dfStore.add {
    dfStore(dfId).withColumnRenamed(existingName, newName)
  }

  /**
    * Selects a set of columns based on expressions.
    */
  override def select(dfId: UUID, columns: Column*): Dataframe = dfStore.add {
    dfStore(dfId).select(columns: _*)
  }

  /**
    * Filters rows using the given condition.
    */
  override def where(dfId: UUID, column: Column): Dataframe = dfStore.add {
    dfStore(dfId).where(column)
  }

  /**
    * Returns a new Dataframe with an alias set.
    */
  override def alias(dfId: UUID, alias: String): Dataframe = dfStore.add {
    dfStore(dfId).alias(alias)
  }

  /**
    * Join with another [[Dataframe]], using the given join expression.
    */
  override def join(leftDfId: UUID, rightDfId: UUID, joinExprs: Column, joinType: String): Dataframe = dfStore.add {
    dfStore(leftDfId).join(dfStore(rightDfId), joinExprs, joinType)
  }

  /**
    * Returns a new [[Dataframe]] by taking the first `n` rows.
    */
  override def limit(dfId: UUID, n: Int): Dataframe = dfStore.add {
    dfStore(dfId).limit(n)
  }

  /**
    * Returns a new Dataframe containing union of rows in this Dataframe and another Dataframe
    * (without deduplication)
    *
    * @group sql-api
    */
  override def union(dfId: UUID, otherDfId: UUID): Dataframe = dfStore.add {
    dfStore(dfId).union(dfStore(otherDfId))
  }

  /**
    * Returns a new Dataframe containing rows only in both this Dataframe and another Dataframe.
    *
    * Note that, equality checking is performed directly on the encoded representation of the data
    * and thus is not affected by a custom `equals` function.
    *
    * @group sql-api
    */
  override def intersect(dfId: UUID, otherDfId: UUID): Dataframe = dfStore.add {
    dfStore(dfId).intersect(dfStore(otherDfId))
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
  override def except(dfId: UUID, otherDfId: UUID): Dataframe = dfStore.add {
    dfStore(dfId).except(dfStore(otherDfId))
  }

  /**
    * Marks a DataFrame as small enough for use in broadcast joins.
    *
    * @group sql-api
    */
  override def broadcast(dfId: UUID): Dataframe = dfStore.add {
    dfStore(dfId).broadcast
  }

  ////////////////////////////////////////////////////////////////////////////////////
  // Aggregation functions
  ////////////////////////////////////////////////////////////////////////////////////

  override def aggregateAgg(dfId: UUID, cols: Seq[Column], aggType: DataframeService.AggregationTypes.AggregationType,
                            pivotColName: Option[String], pivotValues: Option[Seq[Any]],
                            aggExprs: Seq[Column]): Dataframe = dfStore.add {
    agg(dfId, cols, aggType, pivotColName, pivotValues).agg(aggExprs.head, aggExprs.tail: _*)
  }

  override def aggregateCount(dfId: UUID, cols: Seq[Column], aggType: DataframeService.AggregationTypes.AggregationType,
                     pivotColName: Option[String], pivotValues: Option[Seq[Any]]): Dataframe = dfStore.add {
    agg(dfId, cols, aggType, pivotColName, pivotValues).count()
  }

  override def aggregateMin(dfId: UUID, cols: Seq[Column], aggType: DataframeService.AggregationTypes.AggregationType,
                   pivotColName: Option[String], pivotValues: Option[Seq[Any]],
                   minColNames: Seq[String]): Dataframe = dfStore.add {
    agg(dfId, cols, aggType, pivotColName, pivotValues).min(minColNames: _*)
  }

  override def aggregateMean(dfId: UUID, cols: Seq[Column], aggType: DataframeService.AggregationTypes.AggregationType,
                    pivotColName: Option[String], pivotValues: Option[Seq[Any]],
                    meanColNames: Seq[String]): Dataframe = dfStore.add {
    agg(dfId, cols, aggType, pivotColName, pivotValues).mean(meanColNames: _*)
  }

  override def aggregateMax(dfId: UUID, cols: Seq[Column], aggType: DataframeService.AggregationTypes.AggregationType,
                   pivotColName: Option[String], pivotValues: Option[Seq[Any]],
                   maxColNames: Seq[String]): Dataframe = dfStore.add {
    agg(dfId, cols, aggType, pivotColName, pivotValues).max(maxColNames: _*)
  }

  override def aggregateSum(dfId: UUID, cols: Seq[Column], aggType: DataframeService.AggregationTypes.AggregationType,
                   pivotColName: Option[String], pivotValues: Option[Seq[Any]],
                   sumColNames: Seq[String]): Dataframe = dfStore.add {
    agg(dfId, cols, aggType, pivotColName, pivotValues).sum(sumColNames: _*)
  }

  ////////////////////////
  // aggregation helper
  ////////////////////////

  private def agg(dfId: UUID, cols: Seq[Column], aggType: DataframeService.AggregationTypes.AggregationType,
                  pivotColName: Option[String], pivotValues: Option[Seq[Any]]): GroupedDataframe = {
    val gdf1 = aggType match {
      case AggregationTypes.GroupBy =>
        dfStore(dfId).groupBy(cols: _*)
      case AggregationTypes.RollUp =>
        dfStore(dfId).rollup(cols: _*)
      case AggregationTypes.Cube =>
        dfStore(dfId).cube(cols: _*)
    }
    pivotColName.fold(gdf1) { colName =>
      pivotValues.fold(gdf1.pivot(colName)) { values =>
        gdf1.pivot(colName, values)
      }
    }
  }
}
