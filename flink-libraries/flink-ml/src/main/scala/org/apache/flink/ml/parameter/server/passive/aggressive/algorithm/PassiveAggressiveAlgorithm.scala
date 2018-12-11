package org.apache.flink.ml.parameter.server.passive.aggressive.algorithm

import breeze.linalg.SparseVector

/**
  * Common trait for binary and multiclass Passive Aggressive algorithm.
  */
trait PassiveAggressiveAlgorithm[Param, Label, Model] extends Serializable {

  def delta(data: SparseVector[Double],
            model: Model,
            label: Label): Iterable[(Int, Param)]

  def predict(dataPoint: SparseVector[Double], model: Model): Label

}
