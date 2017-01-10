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
  */
trait Stage extends Inputs with Params {

  implicit def ec: ExecutionContext

  def name: String

  /**
    * list of all output slots produced by this component.
    * To be specified by the class that implements this trait
    */
  protected val _outputs: Seq[Slot[PipelineMessage]]

  /**
    * Implement this to do the real job of transforming inputs into outputs
    */
  protected def run(inputs: Seq[PipelineMessage]): Seq[PipelineMessage]

  /////////////////////////////////////////////////////////////////////
  //
  /////////////////////////////////////////////////////////////////////

  /** Lock object for `outputList` */
  private val outputLock: ReadWriteLock = new ReentrantReadWriteLock()

  /** List that holds the outputs */
  private var outputList: Option[Seq[Future[PipelineMessage]]] = None

  /**
    * Return the actual output at the given index.
    */
  def output(idx: Int): Future[PipelineMessage] = {
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
  private def computeOutput: Seq[Future[PipelineMessage]] = {
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

  override def toString: String = s"${getClass.getSimpleName}(name=$name)"
}
