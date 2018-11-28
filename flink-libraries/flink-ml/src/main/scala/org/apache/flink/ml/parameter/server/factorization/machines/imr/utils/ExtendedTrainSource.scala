package org.apache.flink.ml.parameter.server.factorization.machines.imr.utils

import org.apache.flink.ml.parameter.server.factorization.machines.imr.utils.TYPES._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.io.{BufferedSource, Source}

/**
  * Created by lukacsg on 2018.08.29..
  */
class ExtendedTrainSource(dataFilePath: String, header: Boolean = true, randomOrder: Boolean = true) extends SourceFunction[(CompleteProduct, SimpleProduct)] {
  override def cancel(): Unit = ???

  val delimiter = ";"
  val listDelimiter = ","

  private def createList(reader: BufferedSource): List[(ItemId, Array[ItemId], BrandId, Array[CategoryId], Array[WordId])] =
    (if (header) reader.getLines().drop(1)
    else reader.getLines())
      .map(l => {
        val q = l.split(delimiter)
        if (q.length >= 5)
        (q(0).toInt, q(1).split(listDelimiter).map(_.toInt), q(2).toInt, q(3).split(listDelimiter).map(_.toInt), q(4).split(listDelimiter).map(_.toInt))
        else
          (q(0).toInt, q(1).split(listDelimiter).map(_.toInt), -1, Array[CategoryId](), Array[WordId]())
      }).toList

  override def run(ctx: SourceContext[(CompleteProduct, SimpleProduct)]): Unit = {
    val similarities =
      if (randomOrder) scala.util.Random.shuffle(createList(Source.fromFile(dataFilePath)))
      else createList(Source.fromFile(dataFilePath))
    similarities.foreach { case (id, similarArray, brand, categories, words) =>
      val product = CompleteProduct(id, brand, categories, words)
      similarArray.foreach(q => ctx.collect((product, SimpleProduct(q))))
    }
  }
}
