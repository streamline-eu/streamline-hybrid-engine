package org.apache.flink.ml.parameter.server.factorization.machines.imr.experiments

import org.apache.flink.ml.parameter.server.factorization.machines.imr.IMPredict
import org.apache.flink.ml.parameter.server.factorization.machines.imr.utils.TYPES._
import org.apache.flink.ml.parameter.server.factorization.machines.imr.utils.{ModelLoader, nDCGFlatMap}
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object PredictExperiment {

  def main(args: Array[String]): Unit = {
    def parameterCheck(args: Array[String]): Option[String] = {
      def outputNoParamMessage(): Unit = {
        val noParamMsg = "\tUsage:\n\n\t./run <path to parameters file>"
        println(noParamMsg)
      }

      if (args.length == 0 || !(new java.io.File(args(0)).exists)) {
        outputNoParamMessage()
        None
      } else {
        Some(args(0))
      }
    }
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val propsPath = parameterCheck(args).get

    val params = ParameterTool.fromPropertiesFile(propsPath)

    val testPath = params.get("test")
    val modelPath = params.get("model")
    val itemDescriptor = params.get("itemDescriptor")
    val outputFile = params.get("out")
    val results = params.get("result")

    val numFactors = if (params.has("numFactors")) params.getInt("numFactors") else 300
    val mean = if (params.has("mean")) params.getDouble("mean") else 0d
    val stdev = if (params.has("stdev")) params.getDouble("stdev") else 0.01
    val learningRate = if (params.has("learningRate")) params.getDouble("learningRate") else 0.05
    val negativeSampleRate = if (params.has("negativeSampleRate")) params.getInt("negativeSampleRate") else 5
    val topN = if (params.has("topN")) params.getInt("topN") else 100

    val pullLimit = if (params.has("pullLimit")) params.getInt("pullLimit") else 1000
    val workerParallelism = if (params.has("workerParallelism")) params.getInt("workerParallelism") else 20
    val psParallelism = if (params.has("psParallelism")) params.getInt("psParallelism") else 20
    val iterationWaitTime = if (params.has("iterationWaitTime")) params.getInt("iterationWaitTime") else 50000

    val model = env.fromCollection(ModelLoader.linkModel(itemDescriptor, modelPath))
      .rebalance.map(x => x)

    val delimiter = ";"
    val listDelimiter = ","

    val test: DataStream[Prediction] = env.readFile(new TextInputFormat(new Path(testPath)), testPath).map(l => {
      val q = l.split(delimiter)
      Prediction(q(0).toInt, q(1).split(listDelimiter).map(_.toInt))
    }).rebalance.map(x => x)

    val psOut =
      IMPredict
        .predict(
          model,
          test,
          numFactors,
          mean,
          stdev,
          learningRate,
          negativeSampleRate,
          topN,
          pullLimit,
          workerParallelism,
          psParallelism,
          iterationWaitTime)

    psOut
      .writeAsText(results, FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)

    nDCGFlatMap.nDCGToFile(
      psOut
        .map(_ match { case (_, toplist, solution) => (toplist, solution) }), outputFile)

    env.execute()
  }
}
