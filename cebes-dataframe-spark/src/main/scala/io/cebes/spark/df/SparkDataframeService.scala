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
import io.cebes.df.sample.DataSample
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
    * @group sql-api
    */
  override def broadcast(dfId: UUID): Dataframe = dfStore.add {
    dfStore(dfId).broadcast
  }
}
