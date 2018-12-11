package org.apache.flink.ml.parameter.server.passive.aggressive.imr.text.classification

import breeze.linalg._
import org.apache.flink.ml.parameter.server.passive.aggressive.PassiveAggressiveParameterServer
import org.apache.flink.ml.parameter.server.passive.aggressive.PassiveAggressiveParameterServer.OptionLabeledVector
import org.apache.flink.ml.parameter.server.passive.aggressive.algorithm._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala._

class PassiveAggressiveMultiClassTraining

object PassiveAggressiveMultiClassTraining {

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

      val trainingFile = params.getRequired("trainingFile")
      val modelOutputFile = params.getRequired("modelOutputFile")

      val featureCount = params.getRequired("featureCount").toInt
      val labelCount = params.getRequired("labelCount").toInt

      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setBufferTimeout(bufferTimeout)

      type Label = Int
      type FeatureId = Int


      // Parsing the spare vector file
      val trainingDataSource: DataStream[OptionLabeledVector[Int]] = env.readTextFile(trainingFile)
        .map { line =>
          Left(parseLabeled(featureCount)(line)): OptionLabeledVector[Int]
        }
      .setParallelism(readParallelism)

      PassiveAggressiveParameterServer.transformMulticlass(model = None)(
        trainingDataSource,
        workerParallelism = workerParallelism,
        psParallelism = psParallelism,
        featureCount = featureCount,
        rangePartitioning = false,
        passiveAggressiveMethod = paAlgo,
        pullLimit = pullLimit,
        labelCount = labelCount,
        iterationWaitTime = iterationWaitTime)
        .flatMap(x => x match {
          case Right((featureId, paramVector)) =>
            Iterable(writeParameter((featureId, paramVector.toArray)))
          case _ => Iterable()
        })
        .setParallelism(psParallelism)
        .writeAsText(modelOutputFile, FileSystem.WriteMode.OVERWRITE)

      val start = System.currentTimeMillis()
      env.execute()
      println(s"Runtime: ${(System.currentTimeMillis() - start) / 1000} sec")
    })
  }

  type Id = Long

  def parseLabeled(vecLength: Int)(line: String): (SparseVector[Double], Int) = {
    try {
      val split = line.split(" ")
      val labelString = split.head
      val vecStrings = split.tail
      val label = labelString.toInt
      val vec = vecStrings.map(_.split(":") match {
        case Array(idx, value) => (idx.toInt, value.toDouble)
      })

      (SparseVector[Double](vecLength)(vec: _*), label)
    } catch {
      case e@(_: IndexOutOfBoundsException | _: MatchError | _: NumberFormatException) =>
        throw new Exception(s"Error parsing line: $line. Vector should be in the following format: " +
          s"<label> <idx1>:<val1> <idx2>:<val2> ...", e)
    }
  }

  def parseUnlabeledWithId(vecLength: Int)(line: String): (Long, SparseVector[Double]) = {
    try {
      line.split(";") match {
        case Array(vecString, idString) =>
          val id = idString.toLong
          val vecStrings = vecString.split(" ")
          val vec = vecStrings.map(_.split(":") match {
            case Array(idx, value) => (idx.toInt, value.toDouble)
          })

          (id, SparseVector[Double](vecLength)(vec: _*))
      }
    } catch {
      case e@(_: MatchError | _: NumberFormatException) =>
        throw new Exception(s"Error parsing unlabeled vector: $line. Vector should be in the following format: " +
          s"<idx1>:<val1> <idx2>:<val2> ...;<id>", e)
    }
  }

  def writeParameter(paramWithId: (Int, Array[Double])): String = {
    s"${paramWithId._1};${paramWithId._2.mkString(",")}"
  }

  def readParameter(vecLength: Int)(line: String): (Int, Array[Double]) = {
    try {
      line.split(";") match {
        case Array(idString, vecString) =>
          val id = idString.toInt
          val vecStrings = vecString.split(",")
          val vec = vecStrings.map(_.toDouble)

          if (vec.length != vecLength) {
            throw new MatchError()
          }

          (id, vec)
      }
    } catch {
      case e@(_: MatchError | _: NumberFormatException) =>
        throw new Exception(s"Error parsing parameter: $line. Param should be in the following format: " +
          s"<id>;<val1>,<val2>,<val3>...", e)
    }
  }
}
