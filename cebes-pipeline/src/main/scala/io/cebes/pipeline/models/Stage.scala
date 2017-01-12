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

import java.util.concurrent.locks.{ReadWriteLock, ReentrantReadWriteLock}

import scala.concurrent.{ExecutionContext, Future}

/**
  * A Pipeline stage, with a name, inputs, outputs and parameters
  * The Stage is designed around the concept of [[Future]].
  * Stage.output(idx) is the output at slot `idx`, of type [[Future[PipelineMessage]] which can be waited on,
  * and cached if the input doesn't change.
  *
  * Note that this makes the stages to be stateful, which is somehow against the philosophy of Scala,
  * but we do it for the sake of runtime "efficiency",
  * although at the price of more code with all kinds of locks.
  */
trait Stage extends Params {

  /**
    * list of all inputs required by this component.
    * To be specified by the class that implements this trait
    */
  protected val _inputs: Seq[Slot[PipelineMessage]]

  /**
    * list of all output slots produced by this component.
    * To be specified by the class that implements this trait
    */
  protected val _outputs: Seq[Slot[PipelineMessage]]

  /**
    * Implement this to do the real job of transforming inputs into outputs
    */
  protected def run(inputs: Seq[PipelineMessage]): Seq[PipelineMessage]

  /////////////////////////////////////////////////////////////////////////////
  // name of a stage is simply just a StringParam with name "name"
  /////////////////////////////////////////////////////////////////////////////

  /** The name of this stage */
  def getName: String = get(name).get

  /** Convenient method for setting the stage name */
  def setName(stageName: String): this.type = {
    set(name, stageName)
  }

  val name = StringParam("name", None, "Name of the stage, given by the user. Must be unique in a pipeline",
    ParamValidators.isValidStageName)

  /////////////////////////////////////////////////////////////////////////////
  // inputs
  /////////////////////////////////////////////////////////////////////////////

  /** The read-write lock for inputMap */
  protected val inputLock: ReadWriteLock = new ReentrantReadWriteLock()

  /** Internal map for inputs */
  private final val inputMap: SlotMap = SlotMap.empty

  /** The inputMap just got updated */
  @volatile protected var hasNewInput: Boolean = true

  /** Gets an input by its index. */
  private final def getInput[T <: PipelineMessage](inputIdx: Int): Slot[T] = {
    require(0 <= inputIdx && inputIdx < _inputs.size,
      s"Invalid input index: $inputIdx. Has to be in [0, ${_inputs.size})")
    _inputs(inputIdx).asInstanceOf[Slot[T]]
  }

  /** Set a value for the input at the given location */
  def input[T <: PipelineMessage](i: Int, value: Future[T]): this.type = {
    inputLock.writeLock().lock()
    try {
      inputMap.put(getInput(i), value)
      hasNewInput = true
    } finally {
      inputLock.writeLock().unlock()
    }
    this
  }

  /**
    * Run some work on all the input, return a Future[R] where R is the return type of the work.
    * Throw exception if some input is missing, or the input is of the wrong type
    *
    * NOTE: This is reading on the input maps, so callers of this method should acquire
    * the readLock() on `inputLock`
    */
  protected def withAllInputs[R](work: Seq[PipelineMessage] => R)(implicit ec: ExecutionContext): Future[R] = {
    val v = _inputs.map(inp => inputMap(inp))
    Future.sequence(v).map { seq =>
      seq.zip(_inputs).zipWithIndex.foreach { case ((m, s), i) =>
        if (m.getClass != s.messageClass) {
          throw new IllegalArgumentException(
            s"Stage $toString: invalid input type at slot #$i (${s.name}), " +
              s"expected a ${s.messageClass.getSimpleName}, got ${m.getClass.getSimpleName}")
        }
      }
      work(seq)
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  // outputs
  /////////////////////////////////////////////////////////////////////////////

  /** Lock object for `outputList` */
  private val outputLock: ReadWriteLock = new ReentrantReadWriteLock()

  /** List that holds the outputs */
  private var outputList: Option[Seq[Future[PipelineMessage]]] = None

  /**
    * Return the actual output at the given index.
    */
  def output(idx: Int)(implicit ec: ExecutionContext): Future[PipelineMessage] = {
    inputLock.readLock().lock()
    outputLock.readLock().lock()
    if (outputList.isEmpty || hasNewInput) {
      outputLock.readLock().unlock()
      outputLock.writeLock().lock()
      try {
        outputList = Some(computeOutput)
        hasNewInput = false

        // Downgrade by acquiring read lock before releasing write lock
        outputLock.readLock().lock()
      } finally {
        inputLock.readLock().unlock()
        outputLock.writeLock().unlock()
      }
    } else {
      inputLock.readLock().unlock()
    }

    try {
      val outputsVal = outputList.get
      require(0 <= idx && idx < outputsVal.size,
        s"Stage $toString: invalid output index $idx, has to be in [0, ${outputsVal.size})")
      outputsVal(idx)
    } finally {
      outputLock.readLock().unlock()
    }
  }

  /**
    * Compute the output, check the types and number of output slots
    */
  private def computeOutput(implicit ec: ExecutionContext): Seq[Future[PipelineMessage]] = {
    val futureOutput = withAllInputs { inps =>
      val out = run(inps)
      require(out.size == _outputs.size,
        s"Stage $toString has ${_outputs.size} output, but its run() function " +
          s"returns ${out.size} output")
      out.zip(_outputs).zipWithIndex.foreach { case ((m, s), i) =>
        if (s.messageClass != m.getClass) {
          throw new IllegalArgumentException(s"Stage $toString: invalid output type at slot #$i (${s.name}), " +
            s"expected a ${s.messageClass.getSimpleName}, got ${m.getClass.getSimpleName}")
        }
      }
      out
    }
    _outputs.indices.map { i =>
      futureOutput.map { seq =>
        seq(i)
      }
    }
  }

  override def toString: String = s"${getClass.getSimpleName}(name=$getName)"
}
