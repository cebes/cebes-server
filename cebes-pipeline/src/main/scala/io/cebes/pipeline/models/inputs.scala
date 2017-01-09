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

package io.cebes.pipeline.models

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

abstract class Input[+T <: PipelineMessage](val name: String, val doc: String)

case class DataframeInput(override val name: String = "df",
                          override val doc: String = "Dataframe Input") extends Input[DataframeMessage](name, doc)

case class SampleInput(override val name: String = "sample",
                       override val doc: String = "DataSample Input") extends Input[SampleMessage](name, doc)

case class ModelInput(override val name: String = "model",
                      override val doc: String = "Model Input") extends Input[ModelMessage](name, doc)

case class ValueInput(override val name: String = "value",
                      override val doc: String = "Value Input") extends Input[ValueMessage](name, doc)

/**
  * Trait for components that take inputs. This also provides an internal map to store
  * input values attached to the instance.
  */
trait Inputs {

  // list of all inputs required by this component.
  // To be specified by the class that implements this trait
  protected val _inputs: Seq[Input[PipelineMessage]] = Nil

  /** Internal map for inputs */
  private val inputMap: InputMap = InputMap.empty

  /** Gets an input by its name. */
  private def getInput[T <: PipelineMessage](inputName: String): Input[T] = {
    _inputs.find(_.name == inputName).getOrElse {
      throw new NoSuchElementException(s"Input $inputName does not exist.")
    }.asInstanceOf[Input[T]]
  }

  /** Gets an input by its index. */
  private def getInput[T <: PipelineMessage](inputIdx: Int): Input[T] = {
    require(0 <= inputIdx && inputIdx < _inputs.size,
      s"Invalid input index: $inputIdx. Has to be in between 0 and ${_inputs.size}")
    _inputs(inputIdx).asInstanceOf[Input[T]]
  }

  /** Set a value for the input at the given location */
  def input[T <: PipelineMessage](i: Int, value: Future[T]): this.type = {
    val inp = _inputs(i)
    inputMap.put(inp, value)
    this
  }

  protected def withInputs[T <: PipelineMessage, R]
  (inputName: String)(work: T => R)(implicit executor: ExecutionContext, tag: ClassTag[T]): Future[R] = {
    for {
      inp <- inputMap(getInput[T](inputName))
    } yield {
      checkMessageType[T](inp, inputName)
      work(inp)
    }
  }

  protected def withInputs[T1 <: PipelineMessage, T2 <: PipelineMessage, R]
  (inputName1: String, inputName2: String)(work: (T1, T2) => R)
  (implicit executor: ExecutionContext, tag1: ClassTag[T1], tag2: ClassTag[T2]): Future[R] = {
    for {
      inp1 <- inputMap(getInput[T1](inputName1))
      inp2 <- inputMap(getInput[T2](inputName2))
    } yield {
      checkMessageType[T1](inp1, inputName1)
      checkMessageType[T2](inp2, inputName2)
      work(inp1, inp2)
    }
  }

  protected def withInputs[T1 <: PipelineMessage, T2 <: PipelineMessage, T3 <: PipelineMessage, R]
  (inputName1: String, inputName2: String, inputName3: String)(work: (T1, T2, T3) => R)
  (implicit executor: ExecutionContext, tag1: ClassTag[T1], tag2: ClassTag[T2], tag3: ClassTag[T3]): Future[R] = {
    for {
      inp1 <- inputMap(getInput[T1](inputName1))
      inp2 <- inputMap(getInput[T2](inputName2))
      inp3 <- inputMap(getInput[T3](inputName3))
    } yield {
      checkMessageType[T1](inp1, inputName1)
      checkMessageType[T2](inp2, inputName2)
      checkMessageType[T3](inp3, inputName3)
      work(inp1, inp2, inp3)
    }
  }

  protected def withInputs[T <: PipelineMessage, R]
  (idx: Int)(work: T => R)
  (implicit executor: ExecutionContext, tag: ClassTag[T]): Future[R] = {
    for {
      inp <- inputMap(getInput(idx))
    } yield {
      checkMessageType[T](inp, idx)
      work(inp)
    }
  }

  protected def withInputs[T1 <: PipelineMessage, T2 <: PipelineMessage, R]
  (idx1: Int, idx2: Int)(work: (T1, T2) => R)
  (implicit executor: ExecutionContext, tag1: ClassTag[T1], tag2: ClassTag[T2]): Future[R] = {
    for {
      inp1 <- inputMap(getInput[T1](idx1))
      inp2 <- inputMap(getInput[T2](idx2))
    } yield {
      checkMessageType[T1](inp1, idx1)
      checkMessageType[T2](inp2, idx2)
      work(inp1, inp2)
    }
  }

  protected def withInputs[T1 <: PipelineMessage, T2 <: PipelineMessage, T3 <: PipelineMessage, R]
  (idx1: Int, idx2: Int, idx3: Int)(work: (T1, T2, T3) => R)
  (implicit executor: ExecutionContext, tag1: ClassTag[T1], tag2: ClassTag[T2], tag3: ClassTag[T3]): Future[R] = {
    for {
      inp1 <- inputMap(getInput[T1](idx1))
      inp2 <- inputMap(getInput[T2](idx2))
      inp3 <- inputMap(getInput[T3](idx3))
    } yield {
      checkMessageType[T1](inp1, idx1)
      checkMessageType[T2](inp2, idx2)
      checkMessageType[T3](inp3, idx3)
      work(inp1, inp2, inp3)
    }
  }

  private def checkMessageType[T <: PipelineMessage]
  (inp: PipelineMessage, inputName: String)(implicit tag: ClassTag[T]): Unit = {
    val cls = tag.runtimeClass.asInstanceOf[Class[T]]
    require(inp.getClass == cls,
      s"${getClass.getName}: invalid type for input named $inputName, " +
        s"expected ${cls.getSimpleName}, got ${inp.getClass.getSimpleName}")
  }

  private def checkMessageType[T <: PipelineMessage]
  (inp: PipelineMessage, inputIdx: Int)(implicit tag: ClassTag[T]): Unit = {
    val cls = tag.runtimeClass.asInstanceOf[Class[T]]
    require(inp.getClass == cls,
      s"${getClass.getName}: invalid type for input #$inputIdx, " +
        s"expected ${cls.getSimpleName}, got ${inp.getClass.getSimpleName}")
  }
}

/**
  * A map of input to the values.
  * This is similar to [[ParamMap]], however we use Future[T] for the actual value of the input
  */
class InputMap(private val map: mutable.Map[Input[PipelineMessage], Future[PipelineMessage]]) {

  def this() = this(mutable.Map.empty)

  /**
    * Puts a (input, value) pair (overwrites if the input exists).
    */
  def put[T <: PipelineMessage](input: Input[T], value: Future[T]): this.type = {
    map(input.asInstanceOf[Input[T]]) = value
    this
  }

  /**
    * Optionally returns the value associated with an input.
    */
  def get[T <: PipelineMessage](input: Input[T]): Option[Future[T]] = {
    map.get(input.asInstanceOf[Input[T]]).asInstanceOf[Option[Future[T]]]
  }

  /**
    * Returns the value associated with an input or a default value.
    */
  def getOrElse[T <: PipelineMessage](input: Input[T], default: Future[T]): Future[T] = {
    get(input).getOrElse(default)
  }

  /**
    * Gets the value of the input or its default value if it does not exist.
    * Raises a NoSuchElementException if there is no value associated with the given input.
    */
  def apply[T <: PipelineMessage](input: Input[T]): Future[T] = {
    get(input).getOrElse {
      throw new NoSuchElementException(s"Input named ${input.name} is not specified.")
    }
  }
}

object InputMap {

  /**
    * Returns an empty param map.
    */
  def empty: InputMap = new InputMap()

}