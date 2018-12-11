package org.apache.flink.ml.parameter.server.factorization.machines.imr

import org.apache.flink.ml.parameter.server.entities.{PSToWorker, Pull, Push, WorkerToPS}
import org.apache.flink.ml.parameter.server.factorization.machines.factors.RandomGaussianFactorInitializer
import org.apache.flink.ml.parameter.server.factorization.machines.imr.server.PSLogicPerediction
import org.apache.flink.ml.parameter.server.factorization.machines.imr.utils.TYPES.{ItemId, Prediction}
import org.apache.flink.ml.parameter.server.factorization.machines.imr.worker.PSOnlineFMPredictionWorker
import org.apache.flink.ml.parameter.server.matrix.factorization.utils.Vector.Vector
import org.apache.flink.ml.parameter.server.{FlinkParameterServer, WorkerLogic}
import org.apache.flink.streaming.api.scala._

/**
  * Created by lukacsg on 2018.08.31..
  */
object IMPredict {

//  def predict(model: DataStream[Iterable[(ItemId, Vector)]],
  def predict(model: DataStream[(ItemId, Vector)],
                      test: DataStream[Prediction],
                      numFactors: Int = 10,
                      mean: Double = 0,
                      stdev: Double = 0.01,
                      learningRate: Double,
                      negativeSampleRate: Int = 5,
                      topN: Int = 100,
                      pullLimit: Int = 1600,
                      workerParallelism: Int,
                      psParallelism: Int,
                      iterationWaitTime: Long = 10000): DataStream[(ItemId, Seq[(ItemId, Double)], Array[ItemId])] = {
    // model
//    val inputModel = model.forward.flatMap(q => q).forward.map(m => (m._1, Left(m._2)))
    val inputModel = model.forward.map(m => (m._1, Left(m._2)))
      .asInstanceOf[DataStream[(ItemId, Either[Vector, (Int, Vector)])]]

    val workerToPSPartitioner: WorkerToPS[Either[Vector, (Int, Vector)]] => Int = {
      case WorkerToPS(_, msg) =>
        Math.abs(msg match {
          case Right(Push(pId, info)) => info match {
            case Right((partitionNumber, _)) => partitionNumber
            case _ => pId
          }
          case Left(Pull(pId)) => pId
        }) % psParallelism
    }

    val wInPartition: PSToWorker[Either[Vector, (Int, Vector)]] => Int = {
//    val wInPartition = {
      case PSToWorker(workerPartitionIndex, _) => workerPartitionIndex
    }

    val topNList = FlinkParameterServer.transformWithModelLoad(inputModel)(test,
      WorkerLogic.addPullLimiter(new PSOnlineFMPredictionWorker(psParallelism), pullLimit),
      new PSLogicPerediction(_ => RandomGaussianFactorInitializer(mean, stdev).nextFactor(numFactors), topN),
      workerToPSPartitioner,
      wInPartition,
      workerParallelism,
      psParallelism,
      iterationWaitTime)
      // calculate the generic topN
      .flatMap(x => x match {
      case Right(a) => Some(a)
      case _ => None
    }).setParallelism(psParallelism)
      .keyBy(_._1)
      .flatMapWithState[(ItemId, Seq[(ItemId, Double)]), Seq[Seq[(ItemId, Double)]]]((input: (ItemId, Seq[(ItemId, Double)]), state: Option[Seq[Seq[(ItemId, Double)]]]) =>
      state match {
        case None =>
          (None, Some(List(input._2)))
        case Some(a) =>
          if (a.size + 1 < workerParallelism)
            (None, Some(a.++:(List(input._2))))
          else {
            (Some((input._1, a.++(List(input._2)).flatten.sortBy(-_._2).take(topN))), None)
          }
      }).setParallelism(psParallelism)

    // uinion the resoult and the solution
    val toplistEither: DataStream[Either[(ItemId, Array[ItemId]), (ItemId, Seq[(ItemId, Double)])]] = topNList.forward.map(Right(_))
    // filter prediction
    val evaluation: DataStream[Either[(ItemId, Array[ItemId]), (ItemId, Seq[(ItemId, Double)])]] = test.map(q => Left((q.id, q.similarityList)))

    toplistEither.union(evaluation)
      .keyBy(q => q match {
        case Left((id, _)) => id
        case Right((id, _)) => id
      })
      .flatMapWithState[(ItemId, Seq[(ItemId, Double)], Array[ItemId]), Either[(ItemId, Array[ItemId]), (ItemId, Seq[(ItemId, Double)])]](
      (input: Either[(ItemId, Array[ItemId]), (ItemId, Seq[(ItemId, Double)])], state: Option[Either[(ItemId, Array[ItemId]), (ItemId, Seq[(ItemId, Double)])]]) =>
        state match {
          case None =>
            (None, Some(input))
          case Some(a) =>
            a match {
              case Left((_, solution)) =>
                //              assert(input.isRight)
                val e = input.right.get
                (Some((e._1, e._2, solution)), None)
              case Right((_, toplist)) =>
                //              assert(input.isLeft)
                val e = input.left.get
                (Some((e._1, toplist, e._2)), None)
            }
        })
  }

}
