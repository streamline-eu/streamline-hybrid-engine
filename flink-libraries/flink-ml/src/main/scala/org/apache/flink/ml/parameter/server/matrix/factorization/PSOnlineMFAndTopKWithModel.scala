package org.apache.flink.ml.parameter.server.matrix.factorization

import org.apache.flink.ml.parameter.server.FlinkParameterServer
import org.apache.flink.ml.parameter.server.matrix.factorization.factors.{RangedRandomFactorInitializerDescriptor, SGDUpdater}
import org.apache.flink.ml.parameter.server.matrix.factorization.pruning.{COORD, LEMPPruningStrategy}
import org.apache.flink.ml.parameter.server.matrix.factorization.utils.Utils.{ItemId, TopKOutput, UserId}
import org.apache.flink.ml.parameter.server.matrix.factorization.utils.Vector.{LengthAndVector, attachLength, vectorSum}
import org.apache.flink.ml.parameter.server.matrix.factorization.utils.{CollectTopKFromEachLearnWorker, IDGenerator, Rating, RichRating}
import org.apache.flink.ml.parameter.server.matrix.factorization.workers.{BaseMFWorkerLogic, PSOnlineMatrixFactorizationAndTopKGeneratorWorker}
import org.apache.flink.ml.parameter.server.server.SimplePSLogic
import org.apache.flink.api.common.functions.{Partitioner, RichFlatMapFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

class PSOnlineMFAndTopKWithModel {

}

object PSOnlineMFAndTopKWithModel {

  def experiment(model: DataStream[Either[(UserId, LengthAndVector), (ItemId, LengthAndVector)]],
                 src: DataStream[Rating],
                 learningRate: Double,
                 negativeSampleRate: Int,
                 numFactors: Int = 10,
                 rangeMin: Double = -0.01,
                 rangeMax: Double = 0.01,
                 userMemory: Int = 0,
                 K: Int = 100,
                 workerK: Int = 75,
                 bucketSize: Int = 100,
                 pruningAlgorithm: LEMPPruningStrategy = COORD(),
                 pullLimit: Int = 1600,
                 workerParallelism: Int = 4,
                 psParallelism: Int = 4,
                 iterationWaitTime: Int = 10000): DataStream[(ItemId, Long, List[(Double, ItemId)])] = {
    val factorInitDesc = RangedRandomFactorInitializerDescriptor(numFactors, rangeMin, rangeMax)

    val baseWorkerLogic = new PSOnlineMatrixFactorizationAndTopKGeneratorWorker(
      negativeSampleRate = negativeSampleRate,
      userMemory = userMemory,
      workerK = workerK,
      bucketSize = bucketSize,
      pruningAlgorithm = pruningAlgorithm,
      workerParallelism = workerParallelism,
      factorInitDesc = factorInitDesc,
      factorUpdate = new SGDUpdater(learningRate))

    val workerLogic: BaseMFWorkerLogic[RichRating, LengthAndVector, TopKOutput] =
      BaseMFWorkerLogic.addPullLimiter(baseWorkerLogic, pullLimit)

    val serverLogic = new SimplePSLogic[LengthAndVector](
      x => attachLength(factorInitDesc.open().nextFactor(x)),
      { (vec, deltaVec) => attachLength(vectorSum(vec._2, deltaVec._2))}
    )

    val partitionedInput = src.flatMap(new RichFlatMapFunction[Rating, RichRating] {
      override def flatMap(in: Rating, out: Collector[RichRating]) {
        val ratingId = IDGenerator.next
        for (i <- 0 until workerParallelism) {
          out.collect(in.enrich(i, ratingId))
        }
      }
    }).partitionCustom(new Partitioner[Int] {
      override def partition(key: Int, numPartitions: Int): ItemId = { key % numPartitions }
    }, x => x.targetWorker)

    val partitioner = new utils.Partitioner[LengthAndVector](psParallelism)

    FlinkParameterServer.transformWithDoubleModelLoad(model)(
      partitionedInput,
      workerLogic, serverLogic,
      partitioner.workerToPSPartitioner, partitioner.psToWorkerPartitioner,
      workerParallelism, psParallelism, iterationWaitTime)
      .flatMap(new CollectTopKFromEachLearnWorker(K, userMemory, workerParallelism)).setParallelism(1)
      .map( _ match {
        case (_, itemId, timestamp, topK) => (itemId, timestamp, topK)
      })
  }
}
