package org.apache.flink.ml.parameter.server.matrix.factorization.utils


import org.apache.flink.ml.parameter.server.matrix.factorization.utils.Utils._
import org.apache.flink.ml.parameter.server.matrix.factorization.utils.Vector._
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * A flat map function that receives TopKs from each worker, and outputs the overall TopK.
  * Used on data coming out of the parameter server.
  *
  * The Input contains a disjoint union of TopK worker outputs on the Left, and
  * Server outputs on the Right (which will be discarded)
  *
  * The Output contains tuples of user ID, item ID, timestamp, and TopK List
  * @param K Number of items in the generated recommendation
  * @param memory The last #memory item seen by the user will not be recommended
  * @param workerParallelism Number of worker nodes
  */
class CollectTopKFromEachLearnWorker(K: Int, memory: Int, workerParallelism: Int)
    extends RichFlatMapFunction[Either[TopKOutput, (UserId, LengthAndVector)], (UserId, ItemId, Long, List[(Double, ItemId)])] {

  val outputs = new mutable.HashMap[Double, ArrayBuffer[TopKQueue]]
  val seenSet = new mutable.HashMap[UserId, mutable.HashSet[ItemId]]
  val seenList = new mutable.HashMap[UserId, mutable.Queue[ItemId]]

  override def flatMap(value: Either[TopKOutput, (UserId, (VectorLength, Vector))],
                       out: Collector[(UserId, ItemId, Long, List[(Double, ItemId)])]): Unit = {

    value match {
      case Left((RichRating(userId, itemId, _,_, ratingId, timestamp), actualTopK)) =>

        val allTopK = outputs.getOrElseUpdate(ratingId, new ArrayBuffer[TopKQueue]())

        allTopK += actualTopK

        if (allTopKReceived(allTopK)) {
          val topKQueue = allTopK.fold(new mutable.ListBuffer[(Double, ItemId)]())((q1, q2) => q1 ++= q2)
          val topKList = topKQueue.toList
            .filterNot(x => seenSet.getOrElseUpdate(userId, new mutable.HashSet) contains x._2)
            .sortBy(-_._1)
            .take(K)

          out.collect((userId, itemId, timestamp, topKList))
          outputs -= ratingId

          seenSet.getOrElseUpdate(userId, new mutable.HashSet) += itemId
          seenList.getOrElseUpdate(userId, new mutable.Queue) += itemId
          if ((memory > -1) && (seenList(userId).length > memory)) {
            seenSet(userId) -= seenList(userId).dequeue()
          }
        }

      case Right(_) =>
    }
  }

  def allTopKReceived(allTopK: ArrayBuffer[TopKQueue]): Boolean =
    allTopK.size == workerParallelism


}