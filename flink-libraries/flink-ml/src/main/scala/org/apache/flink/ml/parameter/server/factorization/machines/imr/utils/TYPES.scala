package org.apache.flink.ml.parameter.server.factorization.machines.imr.utils

/**
  * Created by lukacsg on 2018.08.29..
  */
object TYPES {

  /**
    * Identifier of items
    */
  type ItemId = Int
  type BrandId = Int
  type CategoryId = Int
  type WordId = Int

  /**
    * Represents an event, which can be an input for the Parameter Server's worker
    */
  sealed trait WorkerInput {
    val id: ItemId
  }

  case class Prediction(id: ItemId, similarityList: Array[ItemId]) extends WorkerInput
  sealed trait TrainingData extends WorkerInput {
    val similar: ItemId
  }

  case class Similarity(id: ItemId, similar: ItemId) extends TrainingData
  case class NegativeSample(id: ItemId, similar: ItemId) extends TrainingData

  sealed trait Product {
    val id: ItemId

    def parameterNumber: Int
    def parameters: Seq[ItemId]
  }

  /// extends worker input
  case class SimpleProduct(id: ItemId) extends Product {
    override def parameterNumber = 1

    override def parameters = Seq(id)
  }
  case class CompleteProduct(id: ItemId, brand: BrandId, categories: Seq[CategoryId], words: Seq[WordId]) extends Product {
    override def parameterNumber = parameters.length

    override def parameters = (Seq(id, brand) ++ categories ++ words).filter(_ >= 0)
  }

  sealed trait ComplexTrainInput {
    val product: CompleteProduct
    val similar: Product
    val parameterNumber = parameterSet.size

    def parameterSet = product.parameters.toSet ++ similar.parameters
  }

  case class Train(product: CompleteProduct, similar: Product) extends ComplexTrainInput
  case class NegativeTrain(product: CompleteProduct, similar: Product) extends ComplexTrainInput



//  sealed trait PsInput {
//    val id: ItemId
//  }
//
//  case class ModelInit(id: ItemId, model: Vector) extends PsInput
//  case class Push(id: ItemId, delta: Vector) extends PsInput
//  case class Predict(id: ItemId, delta: Vector) extends PsInput

}
