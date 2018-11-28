package org.apache.flink.ml.parameter.server.factorization.machines.imr.server

import org.apache.flink.ml.parameter.server.factorization.machines.imr.utils.TYPES.ItemId
import org.apache.flink.ml.parameter.server.matrix.factorization.utils.Vector._
import org.apache.flink.ml.parameter.server.{ParameterServer, ParameterServerLogic}

import scala.collection.mutable

class PSLogicPerediction(paramInit: ItemId => Vector, topN: Int)
  extends ParameterServerLogic[Either[Vector, (Int, Vector)], (ItemId, Seq[(ItemId, Double)])] {

  val params = new mutable.HashMap[Int, Vector]()

  override def onPullRecv(id: ItemId, workerPartitionIndex: Int, ps: ParameterServer[Either[Vector, (Int, Vector)],
    (ItemId, Seq[(ItemId, Double)])]): Unit =
    ps.answerPull(id, Left(params.getOrElseUpdate(id, paramInit(id))), workerPartitionIndex)


  override def onPushRecv(id: ItemId, deltaUpdate: Either[Vector, (Int, Vector)], ps: ParameterServer[Either[Vector, (Int, Vector)],
    (ItemId, Seq[(ItemId, Double)])]): Unit =
    deltaUpdate match {
        // Left side is dedicated for model serving or train
      case Left(v) => params.get(id) match {
          // update / train
        case Some(t) => throw new IllegalStateException("Unexpected state: model has 2 vector for an item")
          // model serving / init
        case None => params += id -> v
      }
        // predict
      case Right(u) =>
        ps.output(id, predict(id, u._2))
    }

  def predict(id: ItemId, v: Vector): Seq[(ItemId, Double)] =
    (if (params.contains(id)) params.filter(_._1 != id)
    else params)
    .map(e => (e._1, dotProduct(e._2, v))).toSeq.sortBy(-_._2).take(topN)
}
