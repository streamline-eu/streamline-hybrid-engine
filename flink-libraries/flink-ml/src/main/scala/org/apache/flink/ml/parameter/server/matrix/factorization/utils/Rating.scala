package org.apache.flink.ml.parameter.server.matrix.factorization.utils

import org.apache.flink.ml.parameter.server.matrix.factorization.utils.Utils.{ItemId, UserId}


/**
  * Rating type for training data
  */
case class Rating(user: UserId, item: ItemId, rating: Double, timestamp: Long) {
  def enrich(workerId: Int, ratingId: Long) = RichRating(user, item, rating, workerId, ratingId, timestamp)
}

/**
  * A rating with a target worker and a rating ID
  */
case class RichRating(user: UserId, item: ItemId, rating: Double, targetWorker: Int, ratingId: Long, timestamp: Long) {
  def reduce() = Rating(user, item, rating, timestamp)
}

object Rating {

  /**
    * Constructs a rating from a tuple
    *
    * @param t
    * A tuple containing a user ID, an item ID, and a rating
    * @return
    * A Rating with the same values and a timestamp of 0
    */
  def fromTuple(t: (Int, Int, Double)): Rating = Rating(t._1, t._2, t._3, 0L)

  def fromArray(t: Array[Long]): Rating = {
    if(t.length != 3){
      throw new IllegalArgumentException
    }
    else
      Rating(t(1).toInt, t(2).toInt, 1.0, t(2))
  }
}