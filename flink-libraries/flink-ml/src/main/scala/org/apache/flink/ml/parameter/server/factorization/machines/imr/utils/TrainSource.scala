package org.apache.flink.ml.parameter.server.factorization.machines.imr.utils

import org.apache.flink.ml.parameter.server.factorization.machines.imr.utils.TYPES.ItemId
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.io.{BufferedSource, Source}

/**
  * Created by lukacsg on 2018.08.29..
  */
class TrainSource(dataFilePath: String, header: Boolean, trainPercent: Int) extends SourceFunction[Either[(ItemId, ItemId), (ItemId, Array[ItemId])]] {
  override def cancel(): Unit = ???

  val delimiter = ";"
  val listDelimiter = ","

  private def createList(reader: BufferedSource): List[(ItemId, Array[ItemId])] =
    (if (header)  reader.getLines().drop(1)
    else reader.getLines())
      .map(l => {
        val q = l.split(delimiter)
        (q(0).toInt, q(1).split(listDelimiter).map(_.toInt))
      }).toList

  override def run(ctx: SourceContext[Either[(ItemId, ItemId), (ItemId, Array[ItemId])]]): Unit = {
    val similarities = scala.util.Random.shuffle(createList(Source.fromFile(dataFilePath)))
    val (train, test) = similarities.splitAt(similarities.size * trainPercent /100)
    train.foreach{case(id, similarArray) =>
      similarArray.foreach(q => ctx.collect(Left(id, q)))
    }
    test.foreach{case(id, similarArray) => ctx.collect(Right(id, similarArray))
    }
  }
}
