package org.apache.flink.ml.parameter.server.factorization.machines.factors

import breeze.numerics.sigmoid
import org.apache.flink.ml.parameter.server.matrix.factorization.utils.Vector._
import org.apache.flink.ml.parameter.server.matrix.factorization.utils.Vector

/**
  * Implement the article:
  * https://www.csie.ntu.edu.tw/~b97053/paper/Rendle2010FM.pdf
  * The SGD updater is implemented only in this point.
  */
object Model {

  def predict(userVect: Vector, itemVects: Array[Vector]): Double =
//    itemVects.map(v => v.zip(userVect).map { case (x, y) => x * y }.sum).sum
    itemVects.map(v => Vector.dotProduct(v, userVect)).sum

  private def gradients(itemVects: Array[Vector]): Vector =
//    itemVects.reduce((a, b) => (a, b).zipped.map(_ + _))
    itemVects.reduce(Vector.vectorSum)


  def deltaSGDUpdater(learningRate: Double, rating: Double, predict: Double, userVect: Vector, itemVects: Array[Vector]): (Vector, Vector) = {
    val errorConst = learningRate * sigmoid(-1 * 2 * (predict - rating))
    (userVect.map(i => errorConst * i), gradients(itemVects).map(i => errorConst * i))
  }

  def deltaSGDUpdater(learningRate: Double, rating: Double, userVect: Vector, itemVects: Array[Vector]): (Vector, Vector) =
    deltaSGDUpdater(learningRate, rating, predict(userVect, itemVects), userVect, itemVects)
}
