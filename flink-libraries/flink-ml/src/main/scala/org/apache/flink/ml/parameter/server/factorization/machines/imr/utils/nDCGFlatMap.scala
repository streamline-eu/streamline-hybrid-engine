package org.apache.flink.ml.parameter.server.factorization.machines.imr.utils

import org.apache.flink.ml.parameter.server.matrix.factorization.utils.Utils.ItemId
import org.apache.flink.ml.parameter.server.matrix.factorization.utils.Vector.VectorLength
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object nDCGFlatMap {

  /**
    * Writes result sum and average nDCG and hit to a human-readable text file.
    * If periodLength is given, results per period are also printed
    * File format:
    *
    * Number of invokes: #<br>
    * Sum nDCG: #<br>
    * Avg nDCG: #<br>
    * Hit: #<br>
    * <br>
    * <br>
    * Period: #<br>
    *     :Number of Invokes: #<br>
    *     :Sum nDCG: #<br>
    *     :Avg nDCG: #<br>
    *     :Hit: #<br>
    * <br>
    * (lines after Period are indented by a tab)
    *
    * @param	topK
    * A flink data stream carrying the item ID and timestamp of the rating that queried the TopK,
    * and a List containing the TopK predicted ratings and item IDs for that user sorted in descending order
    * @param	fileName
    * The name of the output file
    */
  def nDCGToFile(topK: DataStream[(Seq[(ItemId, Double)], Array[ItemId])], fileName: String): Unit =
    topK.flatMap(new nDCGFlatMap).setParallelism(1)
      .writeAsText(fileName, FileSystem.WriteMode.OVERWRITE).setParallelism(1)

/**
  * Evaluates nDCG from TopK output of a recommender system, and writes it to a human-readable text file,
  * or a CSV file that can be used for plotting. Output format of text file:
  *
  * Number of invokes: #<br>
  * Sum nDCG: #<br>
  * Avg nDCG: #<br>
  * Hit: #<br>
  * <br>
  * <br>
  * Period: #<br>
  *     :Number of Invokes: #<br>
  *     :Sum nDCG: #<br>
  *     :Avg nDCG: #<br>
  *     :Hit: #<br>
  * <br>
  * (lines after Period are indented by a tab)
  * In case a non-zero period is specified, there will be a period block for each period.
  *
  * Output format of CSV file in case a period of 0 is specified:
  *
  * invokes,averagenDCG,hitrate
  *
  * in case a non-zero period is specified:
  *
  * periodNumber,invokes,averagenDCG,hitrate
  *
  * @param	fileName
  * Name of the output file
  * @param append
  * Whether to append the file
  *
  */
class nDCGFlatMap
  extends RichFlatMapFunction[(Seq[(ItemId, Double)], Array[ItemId]), String] {


  var sumnDCG = 0.0
  var sumPrecision = 0.0
  var sumRecall = 0.0
  var counter = 0
  var hit = 0

  var collector: Option[Collector[String]] = None

  val log2: VectorLength = Math.log(2)

  private def limit(a: Int, b: Int) = Math.min(a, b)


  override def flatMap(value: (Seq[(ItemId, Double)], Array[ItemId]), out: Collector[String]) {
    if (collector.isEmpty) collector = Some(out)

    val nDCG = value._2.map(e => calculateOneNDCG(e, value._1)).sum / (0 until limit(value._1.length, value._2.length)).map(a => log2 / Math.log(2.0 + a)).sum
    val hits: Double = value._1.map(_._1).count(rec => value._2.contains(rec))
    val precision = hits / value._1.length
    val recall = hits / value._2.length

    if (nDCG != 0)
      hit += 1
    sumnDCG += nDCG
    sumPrecision += precision
    sumRecall += recall
    counter += 1
  }

  private def calculateOneNDCG(candidate: ItemId, toplist: Seq[(ItemId, Double)]) =
    toplist.map(_._1).indexOf(candidate) match {
      case -1 => 0.0
      case i => log2 / Math.log(2.0 + i)
    }

  override def close(): Unit = {
    val avgnDCG = sumnDCG / counter
    val avgPrecision = sumPrecision / counter
    val avgRecall = sumRecall / counter

    val buf = new StringBuilder


    buf ++= s"Number of invokes: $counter\n"
    buf ++= s"Sum nDCG: $sumnDCG\n"
    buf ++= s"Avg nDCG: $avgnDCG\n"
    buf ++= s"Avg precision: $avgPrecision\n"
    buf ++= s"Avg recall: $avgRecall\n"
    buf ++= s"Hit: $hit\n\n"

    collector match {
      case Some(out) => out.collect(buf.toString)
      case _ => throw new IllegalStateException("No collector was be saved.")
    }

  }
}

}
