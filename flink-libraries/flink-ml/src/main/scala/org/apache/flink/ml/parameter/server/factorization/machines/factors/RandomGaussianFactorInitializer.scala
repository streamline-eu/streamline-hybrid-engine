package org.apache.flink.ml.parameter.server.factorization.machines.factors

import org.apache.flink.ml.parameter.server.matrix.factorization.factors.FactorInitializer
import org.apache.flink.ml.parameter.server.matrix.factorization.utils.Vector._

import scala.util.Random

case class RandomGaussianFactorInitializer(mean: Double = 0, stdev: Double = 0.01) extends FactorInitializer {
  override def nextFactor(numFactors: Int): Vector = Array.fill(numFactors)(ran_gaussian(mean, stdev))

  private def ran_gaussian(mean: Double, stdev: Double) = if (stdev == 0) mean
  else mean + stdev*Random.nextGaussian()
}
