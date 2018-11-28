package org.apache.flink.ml.parameter.server.matrix.factorization

import org.apache.flink.ml.parameter.server.matrix.factorization.sinks.nDCGSink
import org.apache.flink.ml.parameter.server.matrix.factorization.utils.Rating
import org.apache.flink.streaming.api.scala._

class PSOnlineMatrixFactorizationAndTopKGeneratorTest {

}

object PSOnlineMatrixFactorizationAndTopKGeneratorTest {

  val numFactors = 10
  val rangeMin = -0.001
  val rangeMax = 0.001
  val learningRate = 0.4
  val userMemory = 0
  val K = 100
  val workerK = 50
  val bucketSize = 10
  val negativeSampleRate = 9
  val pullLimit = 400
  val workerParallelism = 4
  val psParallelism = 4
  val iterationWaitTime = 20000

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val src = env.readTextFile("thesis/input/perf.csv").map(line => {
      val fieldsArray = line.split(",")

      Rating(fieldsArray(1).toInt, fieldsArray(2).toInt, 1.0, fieldsArray(0).toLong)
    })

    val topK = PSOnlineMatrixFactorizationAndTopKGenerator.psOnlineLearnerAndGenerator(
      src,
      numFactors,
      rangeMin,
      rangeMax,
      learningRate,
      negativeSampleRate,
      userMemory,
      K,
      workerK,
      bucketSize,
      pullLimit = pullLimit,
      iterationWaitTime = iterationWaitTime)

    nDCGSink.nDCGToFile(topK, "thesis/output/ndcg/onlineMF_nDCG.csv", 86400)


    env.execute()
  }
}
