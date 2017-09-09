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
package io.cebes.spark.pipeline.ml.traits


import java.nio.file.Paths
import java.util.UUID

import io.cebes.df.Dataframe
import io.cebes.pipeline.json.ModelDef
import io.cebes.pipeline.ml.Model
import io.cebes.pipeline.models.{PipelineMessageSerializer, SlotValueMap}
import io.cebes.spark.df.SparkDataframeFactory
import io.cebes.spark.util.{CebesSparkUtil, SparkSchemaUtils}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util.{MLReadable, MLWritable}

import scala.reflect.runtime.universe
import scala.util.{Failure, Success, Try}


/**
  * Generic trait for Spark Machine Learning model
  *
  * **NOTE**: In order to make serialization works,
  * all subclasses of [[SparkModel]] must have the same primary constructor:
  * (id: UUID, sparkTransformer: Transformer with MLWritable, dfFactory: SparkDataframeFactory)
  *
  */
trait SparkModel extends Model with CebesSparkUtil with SparkSchemaUtils {

  protected val sparkTransformer: Transformer with MLWritable

  protected val dfFactory: SparkDataframeFactory

  override protected def transformImpl(data: Dataframe, params: SlotValueMap): Dataframe = {
    val sparkDf = sparkTransformer.transform(getSparkDataframe(data).sparkDf)

    dfFactory.df(sparkDf, getSchema(sparkDf, data.schema, Seq.empty[String]: _*))
  }

  def toModelDef(msgSerializer: PipelineMessageSerializer, modelStorageDir: String): ModelDef = {
    val sparkModelPath = SparkModel.getStoragePath(modelStorageDir, id)
    sparkTransformer.write.overwrite().save(sparkModelPath)

    val metaData = Map(SparkModel.METADATA_CLASSNAME -> sparkTransformer.getClass.getName)
    val inputs = getInputs(onlyStatefulInput = true).mapValues(msgSerializer.serialize)
    ModelDef(id, getClass.getName, inputs, metaData)
  }
}

object SparkModel {

  private val METADATA_CLASSNAME = s"${getClass.getName}/sparkClassName"

  private def getStoragePath(modelStorageDir: String, id: UUID) = Paths.get(modelStorageDir, id.toString).toString

  /**
    * Create a [[SparkModel]] from a [[ModelDef]]
    * Do not use this function directly. Use an instance of [[io.cebes.pipeline.factory.ModelFactory]] instead.
    *
    * @param modelDef  the [[ModelDef]] to create the model
    * @param dfFactory Dataframe factory
    * @return
    */
  def fromModelDef(modelDef: ModelDef, msgSerializer: PipelineMessageSerializer,
                   dfFactory: SparkDataframeFactory, modelStorageDir: String): SparkModel = {

    val runtimeMirror: universe.Mirror = universe.runtimeMirror(getClass.getClassLoader)

    // load spark transformer
    val sparkClassSymbol = runtimeMirror.classSymbol(Class.forName(modelDef.metaData(METADATA_CLASSNAME)))
    val sparkModuleMirror = runtimeMirror.reflectModule(sparkClassSymbol.companion.asModule)
    val sparkTransformer = sparkModuleMirror.instance.asInstanceOf[MLReadable[Transformer with MLWritable]]
      .load(getStoragePath(modelStorageDir, modelDef.id))

    val classSymbol = runtimeMirror.classSymbol(Class.forName(modelDef.modelClass))
    val classMirror = runtimeMirror.reflectClass(classSymbol)
    val constructorSymbol = classSymbol.primaryConstructor.asMethod

    require(constructorSymbol.paramLists.length == 1,
      s"Primary constructor of model class ${modelDef.modelClass} has more than 1 parameter list")
    val constructorMirror = classMirror.reflectConstructor(constructorSymbol)
    Try(constructorMirror.apply(modelDef.id, sparkTransformer, dfFactory)) match {
      case Success(v) =>
        v match {
          case sparkModel: SparkModel =>
            sparkModel.setInputs(modelDef.inputs.mapValues(msgSerializer.deserialize))
        }
      case Failure(f) =>
        throw new IllegalArgumentException(s"Failed to run constructor for class ${modelDef.modelClass}", f)
    }
  }
}
