package org.apache.flink.ml.parameter.server.factorization.machines.imr.experiments

import org.apache.flink.ml.parameter.server.factorization.machines.imr.SimpleIMPredictAndTrain
import org.apache.flink.ml.parameter.server.factorization.machines.imr.utils.TYPES.ItemId
import org.apache.flink.ml.parameter.server.factorization.machines.imr.utils.nDCGFlatMap
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object SimpleEndToEndExperiment {

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

        val trainPath = params.get("train")
        val modelPath = params.get("model")
        val outputFile = params.get("out")
        val results = params.get("result")

        val numFactors = if (params.has("numFactors")) params.getInt("numFactors") else 300
        val mean = if (params.has("mean")) params.getDouble("mean") else 0d
        val stdev = if (params.has("stdev")) params.getDouble("stdev") else 0.01
        val learningRate = if (params.has("learningRate")) params.getDouble("learningRate") else 0.05
        val negativeSampleRate = if (params.has("negativeSampleRate")) params.getInt("negativeSampleRate") else 5
        val topN = if (params.has("topN")) params.getInt("topN") else 100

        val modelScaling = if (params.has("modelScaling")) params.getDouble("modelScaling") else 0.05 // 0.005

        val pullLimit = if (params.has("pullLimit")) params.getInt("pullLimit") else 1000
        val workerParallelism = if (params.has("workerParallelism")) params.getInt("workerParallelism") else 20
        val psParallelism = if (params.has("psParallelism")) params.getInt("psParallelism") else 20
        val iterationWaitTime = if (params.has("iterationWaitTime")) params.getInt("iterationWaitTime") else 50000

      val trainLength = if (params.has("trainLength")) params.getInt("trainLength") else 5000

      val delimiter = ";"
      val listDelimiter = ","
      var firstModel = true
      val modelDim: Option[Int] = Some(numFactors)
      val scaling: Option[Double] = Some(modelScaling)

      import org.apache.flink.ml.parameter.server.matrix.factorization.utils.Vector._
      val model: DataStream[Iterable[(ItemId, Vector)]] = env.readFile(new TextInputFormat(new Path(modelPath)), modelPath).flatMap(l => {
        if (firstModel) {
          firstModel = false
          None
        } else {
          val q = l.split(delimiter)
          val modelVector = (modelDim match {
            case None => q(1).split(listDelimiter)
            case Some(n) if n <= 300 => q(1).split(listDelimiter).take(n)
            case _ =>
              // TODO: println shoukd be changed to warning log
              println("Model dimension should be under or equal 300")
              q(1).split(listDelimiter)
          }).map(_.toDouble)

          Some(Seq((q(0).toInt, (scaling match {
            case None => modelVector
            case Some(n) => modelVector.map(_ * n)
          }))))
        }
      }).rebalance.map(x => x)


//        val trainTest = env.addSource(new TrainSource(trainPath, header = true, 80))
//          .rebalance.map(x => x)

      var firstTrain = true
      val trainCounterLimit = (trainLength / workerParallelism) * 0.8
      var counter = 0

      val trainTest: DataStream[Either[(ItemId, ItemId), (ItemId, Array[ItemId])]] = env.readFile(new TextInputFormat(new Path(trainPath)), trainPath).flatMap(l => {
        if (firstModel) {
          firstModel = false
          None
        } else {
          val q = l.split(delimiter)
          (q(0).toInt, q(1).split(listDelimiter).map(_.toInt)) match {
            case(id, similarArray) =>
              counter = counter +1
              if (counter < trainCounterLimit) {
                similarArray.map(q => Left(id, q))
              } else {
                Some(Right(id, similarArray))
              }
            case _ => None
          }
        }
      }).rebalance.map(x => x)


      val psOut = SimpleIMPredictAndTrain
        .predictAndTrain(
          model,
          trainTest.setParallelism(workerParallelism),
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

        nDCGFlatMap.nDCGToFile(psOut.map(_ match { case (_, toplist, solution) => (toplist, solution) }), outputFile)

        env.execute()
    }
}
