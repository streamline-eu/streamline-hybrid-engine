package org.apache.flink.ml.parameter.server.factorization.machines.imr.utils

import java.io.{File, PrintWriter}

import org.apache.flink.ml.parameter.server.matrix.factorization.utils.Vector
import org.apache.flink.core.fs.Path

import scala.io.BufferedSource

/**
  * Created by lukacsg on 2018.09.13..
  */
object ModelLoader {

  val delimiter = ";"
  val listDelimiter = ","

  def main(args: Array[String]): Unit = {
    val itemDescriptor           = args(0)
    val modelPath                = args(1)

    val pw = new PrintWriter(new File("/home/lukacsg/data/projects/flink/portugaltelecom/data/internetmemory/linkedModelTest2.out"))
//    linkModel("/mnt/idms/PROJECTS/Streamline/InternetMemory/imr-similar-products-recode/similar_product_extra.csv",
    // "/home/lukacsg/data/projects/flink/portugaltelecom/data/internetmemory/model.out")
    linkModel(itemDescriptor, modelPath)
      .foreach(lm => pw.write(lm._1 + ";" + lm._2.mkString(",") + "\n"))
    pw.close()

  }

  // TODO: check if it is possible not ot represent vector in model
  def linkModel(itemDescriptor: String, modelPath: String): Seq[(Int, Array[Double])] = {
//    val items = createDescriptionList(Source.fromFile(itemDescriptor))
//    val model = createModelMap(Source.fromFile(modelPath))
    val itemPath = new Path(itemDescriptor)
    val itemFs = itemPath.getFileSystem
    val items = createDescriptionList(new BufferedSource(itemFs.open(itemPath)))
    val modelFile = new Path(modelPath)
    val modelFs = modelFile.getFileSystem
    val model = createModelMap(new BufferedSource(modelFs.open(modelFile)))

    items.flatMap {
      case (id, _, brand, category, words) =>
        brand match {
          case Some(b) => val candidate = (Seq(id, b) ++ category ++ words).flatMap(model.get).reduce(Vector.vectorSum)
            if (candidate.isEmpty) None
            else Some((id, candidate))
          case _ => model.get(id) match {
            case Some(e) => Some((id, e))
            case _ => None
          }
        }
    } ++ model.filter(q => (62478 to 76497).contains(q._1))
  }

  private def createDescriptionList(reader: BufferedSource, header: Boolean = true): List[(Int, Array[Int], Option[Int], Array[Int], Array[Int])] =
    (if (header) reader.getLines().drop(1)
    else reader.getLines())
      .map(l => {
        val q = l.split(delimiter)
        //        assert(q.length == 5 || q.length == 2 || q.length == 4)
        q.length match {
          case 2 => (q(0).toInt, q(1).split(listDelimiter).map(_.toInt), None, Array.empty[Int], Array.empty[Int])
          case 4 => (q(0).toInt, q(1).split(listDelimiter).map(_.toInt), Some(q(2).toInt),
            q(3).split(listDelimiter).map(_.toInt), Array.empty[Int])
          case 5=>  (q(0).toInt, q(1).split(listDelimiter).map(_.toInt), Some(q(2).toInt),
            q(3).split(listDelimiter).map(_.toInt), q(4).split(listDelimiter).map(_.toInt))
          case _ => throw new IllegalStateException("Wrong number of fields")
        }
      }).toList


  private def createModelMap(reader: BufferedSource, header: Boolean = true) =
    (if (header) reader.getLines().drop(1)
    else reader.getLines())
      .map(l => {
        val q = l.split(delimiter)
        (q(0).toInt, q(1).split(listDelimiter).map(_.toDouble))
      }).toMap


}
