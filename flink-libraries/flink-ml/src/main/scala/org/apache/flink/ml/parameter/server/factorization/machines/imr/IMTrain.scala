package org.apache.flink.ml.parameter.server.factorization.machines.imr

import org.apache.flink.ml.parameter.server.entities.{PSToWorker, Pull, Push, WorkerToPS}
import org.apache.flink.ml.parameter.server.factorization.machines.factors.RandomGaussianFactorInitializer
import org.apache.flink.ml.parameter.server.factorization.machines.imr.utils.TYPES._
import org.apache.flink.ml.parameter.server.factorization.machines.imr.worker.PSOnlineFMWorker
import org.apache.flink.ml.parameter.server.matrix.factorization.utils.Vector.{Vector, vectorSum}
import org.apache.flink.ml.parameter.server.server.SimplePSLogicWithClose
import org.apache.flink.ml.parameter.server.{FlinkParameterServer, WorkerLogic}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  * Created by lukacsg on 2018.08.31..
  */
object IMTrain {

  def train(model: DataStream[Iterable[(ItemId, Vector)]],
            train: DataStream[(CompleteProduct, SimpleProduct)],
            numFactors: Int = 10,
            mean: Double = 0,
            stdev: Double = 0.01,
            learningRate: Double,
            negativeSampleRate: Int = 5,
            pullLimit: Int = 1600,
            workerParallelism: Int,
            psParallelism: Int,
            iterationWaitTime: Long = 10000): DataStream[(ItemId, Vector)] = {
    // model
    val inputModel = model.forward.flatMap(q => q)
    //train and test
    val src =
      train.forward.flatMap(new FlatMapFunction[(CompleteProduct, SimpleProduct), ComplexTrainInput] {
        val seenItems = new mutable.HashMap[ItemId, CompleteProduct]()


        override def flatMap(value: (CompleteProduct, SimpleProduct), out: Collector[ComplexTrainInput]): Unit = {
          out.collect(Train(value._1, seenItems.get(value._2.id) match {
            case Some(p) => seenItems(value._2.id)
            case None => value._2
          }))
          seenItems += value._1.id -> value._1
          //              negative samples
          createNegativeSamplesRandom(Seq(value._1.id, value._2.id))
            .foreach(n => out.collect(NegativeTrain(value._1, n)))
        }

        // the negativ samples sampling from the list of the seen item
        private def createNegativeSamples(keys: Seq[ItemId]): Seq[Product] = {
          import scala.util.Random
          val possibleNegativeItems = seenItems.keys.filter(keys.contains(_)).toList
          (0 until math.min(negativeSampleRate, possibleNegativeItems.size))
            .foldLeft(List[CompleteProduct]())((l, _) => {
              l :+ seenItems(possibleNegativeItems(Random.nextInt(possibleNegativeItems.size)))
            })
        }

        // the negaiv samples is generated form the complete item id set randomly
        private def createNegativeSamplesRandom(keys: Seq[ItemId]): Seq[Product] = {
          import scala.util.Random
          (1 to negativeSampleRate).map(_ => {
          // max itemid 76497
          var randomItemId = Random.nextInt(76498)
          while(keys contains  randomItemId) randomItemId = Random.nextInt(76498)
          randomItemId
        }).map(randInd => seenItems.get(randInd) match {
            case Some(p) => p
            case None => SimpleProduct(randInd)
          })
        }

      })

    FlinkParameterServer.transformWithModelLoad(inputModel)(src,
      WorkerLogic.addPullLimiter(new PSOnlineFMWorker(learningRate), pullLimit),
      new SimplePSLogicWithClose(_ => RandomGaussianFactorInitializer(mean, stdev).nextFactor(numFactors), vectorSum),
      {
        case WorkerToPS(_, msg) => msg match {
          case Left(Pull(id)) => id % psParallelism
          case Right(Push(id, _)) => id % psParallelism
        }
      }, {
        case PSToWorker(wIdx, _) => wIdx
      },
      workerParallelism,
      psParallelism,
      iterationWaitTime)
      .flatMap(x => x match {
      case Right(a) => Some(a)
      case _ => None
    }).setParallelism(psParallelism)
  }

}
