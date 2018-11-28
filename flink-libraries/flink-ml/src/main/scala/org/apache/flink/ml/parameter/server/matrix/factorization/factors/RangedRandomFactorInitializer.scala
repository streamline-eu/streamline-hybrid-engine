package org.apache.flink.ml.parameter.server.matrix.factorization.factors

import scala.util.Random

class RangedRandomFactorInitializer(numFactors: Int, rangeMin: Double, rangeMax: Double)
  extends FactorInitializer{
  override def nextFactor(id: Int): Array[Double] = {
    val random = new Random(id)
    Array.fill(numFactors)(rangeMin + (rangeMax - rangeMin) * random.nextDouble)
  }
}

case class RangedRandomFactorInitializerDescriptor(numFactors: Int, rangeMin: Double, rangeMax: Double)
  extends FactorInitializerDescriptor{

  override def open(): FactorInitializer =
    new RangedRandomFactorInitializer(numFactors, rangeMin, rangeMax)

}
