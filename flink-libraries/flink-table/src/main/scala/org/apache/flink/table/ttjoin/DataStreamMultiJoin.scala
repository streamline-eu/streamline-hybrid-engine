/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.ttjoin

import java.util

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.prepare.RelOptTableImpl
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{AbstractRelNode, RelNode, RelWriter}
import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode, RexUtil}
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.graph.StreamGraphGenerator
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, PartitionTransformation}
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment, TableException, Trigger}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.sql.JoinedTimeSqlFunction
import org.apache.flink.table.plan.nodes.datastream.DataStreamRel
import org.apache.flink.table.plan.schema.{FlinkTable, RowSchema}
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class DataStreamMultiJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputs: mutable.Seq[RelNode],
    inputsSchema: Seq[RowSchema],
    schema: RowSchema,
    condition: RexNode,
    logicalJoinKeys: Seq[Seq[RexNode]],
    isRowtime: Boolean,
    ruleDescription: String)
  extends AbstractRelNode(cluster, traitSet)
  with DataStreamRel {

  override def deriveRowType(): RelDataType = schema.relDataType

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new DataStreamMultiJoin(
      cluster,
      traitSet,
      inputs,
      inputsSchema,
      schema,
      condition,
      logicalJoinKeys,
      isRowtime,
      ruleDescription)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    inputs.zipWithIndex.foreach(input => pw.input(s"input${input._2}", input._1))

    pw.item("where", joinConditionToString)
      .item("join", joinSelectionToString)
  }

  override def getInputs: util.List[RelNode] = {
    inputs
  }

  override def replaceInput(ordinalInParent: Int, p: RelNode): Unit = {
    inputs(ordinalInParent) = p
  }

  override def getChildExps: util.List[RexNode] = {
    Seq(condition)
  }

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      queryConfig: StreamQueryConfig)
    : DataStream[CRow] = {

    val metadataQuery = RelMetadataQuery.instance()

    // name inputs
    val namedInputs = inputs.zipWithIndex.map { case (input, index) =>
      if (index > Byte.MaxValue) {
        throw TableException(s"More than ${Byte.MaxValue} inputs are not supported.")
      }
      (index.toByte, input)
    }

    // determine row counts per input
    val inputRowCountMap = new Array[Double](namedInputs.length)
    namedInputs.foreach { case (index, input) =>
      val rowCount = metadataQuery.getRowCount(input)
      inputRowCountMap(index) = rowCount
    }

    // extract equation pairs and ignore time indicators
    // pair looks like (single key, single key)
    // but with full indices over entire join row type and time indicator shift
    val fullLogicalPairs = RexUtil.flattenAnd(Seq(condition)).flatMap { expr =>
      // ignore joined time call
      if (RexUtil.isCallTo(expr, JoinedTimeSqlFunction.INSTANCE)) {
        None
      }
      else if (!RexUtil.isCallTo(expr, SqlStdOperatorTable.EQUALS)) {
        throw new TableException("Only equality predicates are supported for joins.")
      } else {
        val ops = expr.asInstanceOf[RexCall].getOperands
        if (ops.exists(!RexUtil.isReferenceOrAccess(_, false))) {
          throw new TableException("A join predicate should only contain field references.")
        }
        val opRefs = ops.map(_.asInstanceOf[RexInputRef])
        Some((opRefs.head.getIndex, opRefs(1).getIndex))
      }
    }

    // extract key sets
    // a key set contains a set of single keys with full join row index and time indicator shift
    val fullLogicalKeySets = mutable.ArrayBuffer[mutable.Set[Int]]()
    fullLogicalPairs.foreach { case (left, right) =>
        val transitiveSet = fullLogicalKeySets
          .find(keySet => keySet.contains(left) || keySet.contains(right))
        transitiveSet match {
          // set exists
          case Some(foundSet) =>
            foundSet += left
            foundSet += right
          // create new set
          case _ =>
            fullLogicalKeySets += mutable.Set[Int](left, right)
        }
    }

    // determine distinct middle keys per input (index starting at 0)
    // keys are not moved to the front, there can be gaps of values in between
    val inputMiddleKeysMap = new Array[Array[Int]](namedInputs.length)
    namedInputs.foreach { case (index, _) =>
      val middleKeys = logicalJoinKeys(index)
        .map(_.asInstanceOf[RexInputRef].getIndex)
        .distinct
      inputMiddleKeysMap(index) = middleKeys.toArray
    }

    // determine non-join middle values (index starting at 0)
    // values are not moved to the back, there can be gaps of keys in between
    val inputMiddleValuesMap = new Array[Array[Int]](namedInputs.length)
    namedInputs.foreach { case (index, _) =>
      val schema = inputsSchema(index)
      val middleKeys = inputMiddleKeysMap(index)
      val middleValues = for (i <- 0 until schema.arity if !middleKeys.contains(i)) yield i
      inputMiddleValuesMap(index) = middleValues.toArray
    }

    // determine key sets for converted input
    // per key set: (input, frontKey)
    val keySetInputsFrontKeysMap = new Array[Array[Array[Int]]](fullLogicalKeySets.length)
    fullLogicalKeySets.zipWithIndex.foreach { case (keySet, keySetIndex) =>
      val inputsFrontKeys = new Array[Array[Int]](keySet.size)
      keySet.zipWithIndex.foreach { case (fullMiddleKey, inKeySetIndex) =>
        // convert full middle key over all inputs into relative front key per input
        var curInputIndex = 0
        var offset = 0
        while (fullMiddleKey >= (offset + inputsSchema(curInputIndex).arity) &&
            curInputIndex < namedInputs.length) {
          offset += inputsSchema(curInputIndex).arity
          curInputIndex += 1
        }
        val relativeMiddleKey = fullMiddleKey - offset
        // map to conversion where keys are moved to the front
        val convertedFrontKey = inputMiddleKeysMap(curInputIndex).indexOf(relativeMiddleKey)
        inputsFrontKeys(inKeySetIndex) = Array(curInputIndex, convertedFrontKey)
      }
      keySetInputsFrontKeysMap(keySetIndex) = inputsFrontKeys
    }

    // determine key sets per input and front key
    val inputFrontKeyKeySetMap = new Array[Array[Int]](namedInputs.length)
    // prepare array
    namedInputs.foreach { case (index, _) =>
      val frontKeyKeySetMap = new Array[Int](inputMiddleKeysMap(index).length)
      inputFrontKeyKeySetMap(index) = frontKeyKeySetMap
    }
    // fill array with conversion of key set input front keys map
    keySetInputsFrontKeysMap.zipWithIndex.foreach { case (keySet, keySetIndex) =>
      keySet.foreach { case Array(frontKeyInput, frontKey) =>
        inputFrontKeyKeySetMap(frontKeyInput)(frontKey) = keySetIndex
      }
    }

    // determine distinct values for each front key
    val inputFrontKeyDistinctCountMap = new Array[Array[Double]](namedInputs.length)
    namedInputs.foreach { case (index, input) =>
      // get distinct count for all keys of input
      val frontKeyDistinctCountMap = new Array[Double](inputMiddleKeysMap(index).length)
      inputMiddleKeysMap(index).foreach { middleKey =>
        val column = ImmutableBitSet.builder().set(middleKey).build()
        val distinctRowCount = metadataQuery.getDistinctRowCount(input, column, null)
        val distinctCount = if (distinctRowCount != null) {
          distinctRowCount.toDouble
        } else {
          inputRowCountMap(index)
        }
        // map to conversion where keys are moved to the front
        val convertedFrontKey = inputMiddleKeysMap(index).indexOf(middleKey)
        frontKeyDistinctCountMap(convertedFrontKey) = distinctCount
      }
      inputFrontKeyDistinctCountMap(index) = frontKeyDistinctCountMap
    }

    // determine total key order
    val totalKeyOrder = new CubeCellJoinOrder(
        inputRowCountMap,
        inputFrontKeyDistinctCountMap,
        keySetInputsFrontKeysMap
      ).getBestTotalOrder

    // create row mapping where keys are at the front of the record
    // and ordered according to total key order
    // input -> (front key), (front key), ...
    val inputOrderedFrontKeysMap = new Array[Array[Int]](namedInputs.length)
    inputFrontKeyKeySetMap.zipWithIndex.foreach { case (frontKeyKeySetMap, inputIndex) =>
      val frontKeys = new Array[Int](frontKeyKeySetMap.length)
      var i = 0
      totalKeyOrder.foreach { keySet =>
        if (frontKeyKeySetMap.contains(keySet)) {
          frontKeys(i) = frontKeyKeySetMap.indexOf(keySet)
          i += 1
        }
      }
      inputOrderedFrontKeysMap(inputIndex) = frontKeys
    }

    // create mapping of middleKeys to orderKeys
    val inputOrderKeyMiddleKeyMap = new Array[Array[Int]](namedInputs.length)
    inputMiddleKeysMap.zipWithIndex.foreach { case (middleKeys, inputIndex) =>
      val orderKeyMiddleKeyMap = new Array[Int](middleKeys.length)
      middleKeys.zipWithIndex.foreach { case (middleKey, frontKey) =>
        val orderKey = inputOrderedFrontKeysMap(inputIndex).indexOf(frontKey)
        orderKeyMiddleKeyMap(orderKey) = middleKey
      }
      inputOrderKeyMiddleKeyMap(inputIndex) = orderKeyMiddleKeyMap
    }

    // ordered version of inputFrontKeyKeySetMap
    val inputOrderKeyKeySetMap = new Array[Array[Int]](namedInputs.length)
    inputFrontKeyKeySetMap.zipWithIndex.foreach { case (frontKeyKeySetMap, inputIndex) =>
      val orderKeyKeySetMap = new Array[Int](frontKeyKeySetMap.length)
      frontKeyKeySetMap.zipWithIndex.foreach { case (keySet, frontKey) =>
        val orderKey = inputOrderedFrontKeysMap(inputIndex).indexOf(frontKey)
        orderKeyKeySetMap(orderKey) = keySet
      }
      inputOrderKeyKeySetMap(inputIndex) = orderKeyKeySetMap
    }

    // determine skew for each order key
    val inputOrderKeySkewMap = new Array[Array[Boolean]](namedInputs.length)
    namedInputs.foreach { case (index, input) =>
      // get skewness for all keys of input
      val orderKeySkewMap = new Array[Boolean](inputMiddleKeysMap(index).length)
      inputMiddleKeysMap(index).foreach { middleKey =>
        // get skewness for not-moved middle key
        val columnOrigin = metadataQuery.getColumnOrigin(input, middleKey)
        val skewness: Boolean = if (columnOrigin == null) {
          false
        } else {
          val table = columnOrigin
            .getOriginTable.asInstanceOf[RelOptTableImpl]
            .getTable.asInstanceOf[FlinkTable[_]]

          val columnName = table
            .getRowType(getCluster.getTypeFactory)
            .getFieldNames
            .get(columnOrigin.getOriginColumnOrdinal)

          val columnStats = table
            .getStatistic
            .getColumnStats(columnName)

          if (columnStats != null) {
            columnStats.skewness
          } else {
            false
          }
        }
        // map to conversion where keys are moved to the front and ordered
        val orderKey = inputOrderKeyMiddleKeyMap(index).indexOf(middleKey)
        orderKeySkewMap(orderKey) = skewness
      }
      inputOrderKeySkewMap(index) = orderKeySkewMap
    }

    // generate hyper cube
    val maxParallelism = if (tableEnv.execEnv.getMaxParallelism < 0) {
      StreamGraphGenerator.DEFAULT_LOWER_BOUND_MAX_PARALLELISM
    } else {
      tableEnv.execEnv.getMaxParallelism
    }
    val hybridHyperCube = new HybridHyperCube(
      fullLogicalKeySets.length,
      maxParallelism,
      inputRowCountMap,
      inputOrderKeyKeySetMap,
      inputOrderKeySkewMap)

    val bestMaxParallelism = hybridHyperCube.getBestMaxParallelism
    val dimensionCount = hybridHyperCube.getDimensionCount
    val dimensionSizes = hybridHyperCube.getDimensionSizes
    val tableKeyDimensionMap = hybridHyperCube.getTableKeyDimensionMap
    val tablePosGroupMap = hybridHyperCube.getTablePosGroupMap
    val groupCellsMap = hybridHyperCube.getGroupCellsMap

    // create pre-processing type
    val inputConversionMap = new Array[(JoinRecordTypeInfo, Array[Int])](namedInputs.length)
    namedInputs.foreach { case (index, _) =>
      val schema = inputsSchema(index)
      val orderedMiddleKeys = inputOrderKeyMiddleKeyMap(index)
      val middleValues = inputMiddleValuesMap(index)
      // e.g. (0, 3) ++ (1, 2) => (0, 3, 1, 2)
      val mapping = orderedMiddleKeys ++ middleValues
      val fieldTypes = mapping.map(fieldIdx => schema.fieldTypeInfos(fieldIdx))
      val payloadRowTypeInfo = new RowTypeInfo(fieldTypes: _*)
      val preProcessorResultType = new JoinRecordTypeInfo(payloadRowTypeInfo)
      inputConversionMap(index) = (preProcessorResultType, mapping)
    }

    // translate inputs physically
    val preProcessedInputs = namedInputs.map { case (index, input) =>
      val inputStream = input
        .asInstanceOf[DataStreamRel]
        .translateToPlan(tableEnv, queryConfig)

      val preProcessingType = inputConversionMap(index)._1
      val conversionMap = inputConversionMap(index)._2

      val rowtimeIndex = if (isRowtime) {
        val idx = preProcessingType
          .payloadRowTypeInfo
          .getFieldTypes
          .indexWhere { ti =>
            FlinkTypeFactory.isRowtimeIndicatorType(ti)
          }
        if (idx < 0) {
          throw TableException("Could not find time attribute. This should not happen.")
        }
        idx
      } else {
        -1
      }

      // transform with join preprocessor
      inputStream
        .transform(
          "MultiJoinPreProcessor" + index,
          preProcessingType,
          new MultiJoinPreprocessorOperator(
            index,
            conversionMap,
            dimensionCount,
            dimensionSizes,
            tableKeyDimensionMap(index),
            tablePosGroupMap.get(index),
            rowtimeIndex
          )
        )
        .setParallelism(inputStream.getParallelism) // avoid an additional rebalance
        .asInstanceOf[DataStream[JoinRecord]]
    }

    // union all inputs
    val unionedInputs = preProcessedInputs.reduceLeft(_.union(_))

    // custom key group partitioning
    val partitionedStream = new DataStream[JoinRecord](
      unionedInputs.getExecutionEnvironment,
      new PartitionTransformation[JoinRecord](
        unionedInputs.getTransformation,
        new HybridHyperCubeStreamPartitioner(bestMaxParallelism)
      )
    )

    // determine field type information
    val inputFieldTypesMap = new Array[Array[TypeInformation[_]]](namedInputs.length)
    inputConversionMap.zipWithIndex.foreach { case (conversion, inputIndex) =>
      inputFieldTypesMap(inputIndex) = conversion._1.payloadRowTypeInfo.getFieldTypes
    }

    // determine output mapping (from order keys to middle keys)
    val outputMap = new Array[Array[Int]](schema.arity)
    var fieldOffset = 0
    namedInputs.foreach { case (input, _) =>
      outputMap(input) = new Array[Int](inputFieldTypesMap(input).length)
      // add total order keys to output map
      inputOrderKeyMiddleKeyMap(input).zipWithIndex.foreach { case (middleKey, orderKey) =>
        outputMap(input)(orderKey) = fieldOffset + middleKey
      }

      val orderKeyCount = inputOrderKeyMiddleKeyMap(input).length

      // add values to output map
      inputMiddleValuesMap(input).foreach { middleValue =>
        val orderValue = orderKeyCount + inputMiddleValuesMap(input).indexOf(middleValue)
        outputMap(input)(orderValue) = fieldOffset + middleValue
      }

      fieldOffset += inputsSchema(input).arity
    }

    // transform with join processor
    val processedStream = partitionedStream
        .transform(
          "MultiJoinProcessor",
          CRowTypeInfo(schema.typeInfo),
          new MultiJoinProcessorOperator(
            isEventTime = isRowtime,
            queryConfig.getTrigger,
            queryConfig.getTriggerPeriod,
            groupCellsMap,
            namedInputs.length,
            inputFieldTypesMap,
            totalKeyOrder,
            inputOrderKeyKeySetMap,
            outputMap
          )
        )
        .setMaxParallelism(bestMaxParallelism)
        .asInstanceOf[DataStream[CRow]]

    val processedTransformation = processedStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[JoinRecord, CRow]]

    processedTransformation.setStateKeyType(BasicTypeInfo.INT_TYPE_INFO)
    processedTransformation.setStateKeySelector(new KeySelector[JoinRecord, Int] {
      override def getKey(value: JoinRecord): Int = {
        throw new RuntimeException("This should never be called.")
      }
    })

    // DEBUG
//    printKeySets(fullLogicalKeySets)
//    printOrderedKeySets(fullLogicalKeySets, totalKeyOrder)
//    println(s"Number of cell groups: ${groupCellsMap.length}")
//    println(s"Cell Groups: \n${printCellGroups(groupCellsMap)}")

    processedStream
  }

  private def printKeySets(fullLogicalKeySets: mutable.ArrayBuffer[mutable.Set[Int]]): Unit = {
    val str = fullLogicalKeySets.zipWithIndex.map {
      case (keySet, keySetIdx) =>
        s"$keySetIdx = {" + keySet.map(getRowType.getFieldNames()(_)) + "}"
    }.mkString("\n")
    println("KEYSETS: \n" + str + "\n\n")
  }

  private def printOrderedKeySets(fullLogicalKeySets: mutable.ArrayBuffer[mutable.Set[Int]], totalKeyOrder: Array[Int]): Unit = {
    val str = totalKeyOrder.map { keySet =>
      s"{" + fullLogicalKeySets(keySet).map(getRowType.getFieldNames()(_)) + "}"
    }.mkString("\n")
    println("TOTAL ORDER KEYSETS: \n" + str + "\n\n")
  }

  private def printCellGroups(groupCellsMap: Array[Array[Int]]): String = {
    val str = groupCellsMap.zipWithIndex.map { case (cell, idx) =>
      s"$idx: ${cell.mkString(", ")}"
    }.mkString("\n")

    str
  }

  private def joinSelectionToString: String = {
    getRowType.getFieldNames.mkString(", ")
  }

  private def joinConditionToString: String = {
    val inFields = schema.relDataType.getFieldNames.toList
    getExpressionString(condition, inFields, None)
  }
}
