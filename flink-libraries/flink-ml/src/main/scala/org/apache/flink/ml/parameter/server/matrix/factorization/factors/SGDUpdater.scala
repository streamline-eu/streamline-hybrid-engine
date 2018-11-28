package org.apache.flink.ml.parameter.server.matrix.factorization.factors
import breeze.numerics.sigmoid

class SGDUpdater(learningRate: Double) extends FactorUpdater {

  override def delta(rating: Double, user: Array[Double], item: Array[Double]): (Array[Double], Array[Double]) = {
    val e = sigmoid(rating - user.zip(item).map { case (x, y) => x * y }.sum)

    (item.map(i => learningRate * e * i),
     user.map(u => learningRate * e * u))
  }

}
