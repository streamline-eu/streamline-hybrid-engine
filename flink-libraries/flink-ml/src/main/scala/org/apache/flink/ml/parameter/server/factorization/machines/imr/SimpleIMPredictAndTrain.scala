package org.apache.flink.ml.parameter.server.factorization.machines.imr

import org.apache.flink.ml.parameter.server.entities.{PSToWorker, Pull, Push, WorkerToPS}
import org.apache.flink.ml.parameter.server.factorization.machines.factors.RandomGaussianFactorInitializer
import org.apache.flink.ml.parameter.server.factorization.machines.imr.server.SimplePSLogic
import org.apache.flink.ml.parameter.server.factorization.machines.imr.utils.TYPES._
import org.apache.flink.ml.parameter.server.factorization.machines.imr.worker.SimplePSOnlineFMWorker
import org.apache.flink.ml.parameter.server.matrix.factorization.utils.Vector.{Vector, vectorSum}
import org.apache.flink.ml.parameter.server.{FlinkParameterServer, WorkerLogic}
import org.apache.flink.streaming.api.scala._

import scala.util.Random

/**
  * Created by lukacsg on 2018.08.31..
  */
object SimpleIMPredictAndTrain {

  def predictAndTrain(model: DataStream[Iterable[(ItemId, Vector)]],
                      trainAndTest: DataStream[Either[(ItemId, ItemId), (ItemId, Array[ItemId])]],
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
    val inputModel = model.forward.flatMap(q => q).forward.map(m => (m._1, Left(m._2)))
      .asInstanceOf[DataStream[(ItemId, Either[Vector, (Int, Vector)])]]
    //train and test
    val src =
    trainAndTest.forward.map(d => d match {
      case Left((id, similar)) =>
        Similarity(id, similar)
      case Right((id, similarityList)) =>
        Prediction(id, similarityList)
    })
    // assumed that item id is in 0-76497
    // generate negativ samples
    val inputSrc: DataStream[WorkerInput] = src.forward.flatMap(q => q match {
//    val inputSrc = src.flatMap(q => q match {
      case p: Prediction => Some(p)
      case t@Similarity(id, similar) =>
        val negativSamples = (1 to negativeSampleRate).map(_ => {
          // max itemid 76497
          var randomItemId = Random.nextInt(76498)
          while(randomItemId == similar) randomItemId = Random.nextInt(76498)
          randomItemId
        }).map(NegativeSample(id, _))
        List(t) ++ negativSamples
      case _ => throw new IllegalArgumentException("Invalid type comes up")
    })

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

    val topNList = FlinkParameterServer.transformWithModelLoad(inputModel)(inputSrc,
      WorkerLogic.addPullLimiter(new SimplePSOnlineFMWorker(learningRate, psParallelism), pullLimit),
      new SimplePSLogic(_ => RandomGaussianFactorInitializer(mean, stdev).nextFactor(numFactors), vectorSum, topN),
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
    val evaluation: DataStream[Either[(ItemId, Array[ItemId]), (ItemId, Seq[(ItemId, Double)])]] = src.filter(q => q match {
      case _: Prediction => true
      case _ => false
    }).asInstanceOf[DataStream[Prediction]].map(q => Left((q.id, q.similarityList)))

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
