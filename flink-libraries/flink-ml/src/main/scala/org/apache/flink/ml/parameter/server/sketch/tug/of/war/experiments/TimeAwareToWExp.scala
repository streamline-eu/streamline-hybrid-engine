package org.apache.flink.ml.parameter.server.sketch.tug.of.war.experiments

import org.apache.flink.ml.parameter.server.sketch.tug.of.war.TimeAwareTugOfWar
import org.apache.flink.ml.parameter.server.sketch.utils.TimeAwareTweetReader
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala._

class TimeAwareToWExp {

}

object TimeAwareToWExp {

  def main(args: Array[String]): Unit = {
    val srcFile = args(0)
    val searchWordsFile = args(1)
    val delimiter = args(2)
    val modelFile = args(3)
    val workerParallelism = args(4).toInt
    val psParallelism = args(5).toInt
    val iterationWaitTime = args(6).toLong
    val numHashes = args(7).toInt
    val startTimestamp = args(8).toLong
    val windowSize = args(9).toInt // in hours


    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val searchWords = scala.io.Source.fromFile(searchWordsFile).getLines.toList

    val src = env
      .readTextFile(srcFile)
      .flatMap(new TimeAwareTweetReader(delimiter, searchWords, startTimestamp, windowSize))


    TimeAwareTugOfWar.tugOfWar(
      src,
      numHashes,
      workerParallelism,
      psParallelism,
      iterationWaitTime
    ).map(value => s"${value._1}:${value._2.mkString(",")}")
      .writeAsText(modelFile, FileSystem.WriteMode.OVERWRITE)

    env.execute()
  }
}
