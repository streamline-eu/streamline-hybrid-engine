package org.apache.flink.ml.parameter.server.passive.aggressive.imr.text.classification

import breeze.linalg.Vector
import org.apache.flink.ml.parameter.server.passive.aggressive.PassiveAggressiveParameterServer
import org.apache.flink.ml.parameter.server.passive.aggressive.algorithm.{PassiveAggressiveOneVersusAll, PassiveAggressiveOneVersusAllImpl, PassiveAggressiveOneVersusAllImplI, PassiveAggressiveOneVersusAllImplII}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala._

object PassiveAggressivePrediction {

  def parseModel(line: String): (Int, Vector[Double]) = {
    val id :: params :: Nil = line.split(";").toList

    (id.toInt, Vector(params.split(",").map(_.toDouble)))

  }

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

    parameterCheck(args).foreach(propsPath => {
      val params = ParameterTool.fromPropertiesFile(propsPath)
      // Parameters that effect the training algorithm
      //    PassiveAggressiveFilter type can be only in the set (0, 1, 2)
      val paType = Option(params.getRequired("paType")) match {
        case Some(q) => q.toInt
        case None => 0
      }

      //    if necessary to the algorithm
      val paAggressiveness = Option(params.getRequired("paAggressiveness")) match {
        case Some(q) => q.toDouble
        case None => 0
      }

      val paAlgo: PassiveAggressiveOneVersusAll = paType match {
        case 0 => new PassiveAggressiveOneVersusAllImpl()
        case 1 => new PassiveAggressiveOneVersusAllImplI(paAggressiveness)
        case 2 => new PassiveAggressiveOneVersusAllImplII(paAggressiveness)
      }

      // Number of worker and PS instances
      val workerParallelism = params.getRequired("workerParallelism").toInt
      val psParallelism = params.getRequired("psParallelism").toInt
      val readParallelism = params.getRequired("readParallelism").toInt

      val bufferTimeout = params.getRequired("bufferTimeout").toLong
      val iterationWaitTime = params.getRequired("iterationWaitTime").toLong
      // TODO make pull limit optional
      val pullLimit = params.getRequired("pullLimit").toInt

      val unlabeledFile = params.getRequired("unlabeledFile")
      val modelFile = params.getRequired("modelFile")
      val predictionOutputFile = params.getRequired("predictionOutputFile")

      val featureCount = params.getRequired("featureCount").toInt
      val labelCount = params.getRequired("labelCount").toInt

      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setBufferTimeout(bufferTimeout)

      type Label = Int
      type FeatureId = Int


      import PassiveAggressiveParameterServer.OptionLabeledVector

      val modelDataSource = env
        .readTextFile(modelFile)
        .map(parseModel(_))

      // Parsing the spare vector file
      val unlabeledSource: DataStream[OptionLabeledVector[Int]] = env.readTextFile(unlabeledFile)
        .map { line =>
          Right(PassiveAggressiveMultiClassTraining.parseUnlabeledWithId(featureCount)(line)): OptionLabeledVector[Int]
        }
        .setParallelism(readParallelism)

      PassiveAggressiveParameterServer.transformMulticlassWithLongId(model = Some(modelDataSource))(
        unlabeledSource,
        workerParallelism = workerParallelism,
        psParallelism = psParallelism,
        featureCount = featureCount,
        rangePartitioning = false,
        passiveAggressiveMethod = paAlgo,
        pullLimit = pullLimit,
        labelCount = labelCount,
        iterationWaitTime = iterationWaitTime)
        .flatMap(x => x match {
          case Left((id, label)) =>
            Iterable(s"$id;$label")
          case _ => Iterable()
        })
        .setParallelism(psParallelism)
        .writeAsText(predictionOutputFile, FileSystem.WriteMode.OVERWRITE)

      val start = System.currentTimeMillis()
      env.execute()
      println(s"Runtime: ${(System.currentTimeMillis() - start) / 1000} sec")
    })
  }

}
