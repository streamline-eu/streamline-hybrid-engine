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

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.rules.MultiJoin
import org.apache.calcite.rel.{AbstractRelNode, RelNode, RelWriter}
import org.apache.calcite.rex.{RexCall, RexNode}
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.calcite.FlinkTypeFactory.{isProctimeIndicatorType, isRowtimeIndicatorType}
import org.apache.flink.table.functions.sql.JoinedTimeSqlFunction
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalRel

import scala.collection.JavaConversions._
import scala.collection.mutable

class FlinkLogicalMultiJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputs: mutable.Seq[RelNode],
    val condition: RexNode,
    val joinKeys: Seq[Seq[RexNode]],
    val isRowtime: Boolean,
    resultType: RelDataType)
  extends AbstractRelNode(cluster, traitSet)
  with FlinkLogicalRel {

  override def deriveRowType(): RelDataType = resultType

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new FlinkLogicalMultiJoin(
      cluster,
      traitSet,
      inputs,
      condition,
      joinKeys,
      isRowtime,
      resultType)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    inputs.zipWithIndex.foreach(input => pw.input(s"input${input._2}", input._1))

    pw.item("where", joinConditionToString)
      .item("join", joinSelectionToString)
  }

  override def computeSelfCost (planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val rowCnt = inputs.map(metadata.getRowCount)
    val rowSize = inputs.map(input => estimateRowSize(input.getRowType))

    val ioCost = rowCnt.zip(rowSize).map(f => f._1 * f._2).sum
    val cpuCost = rowCnt.reduce(_ + _)
    val rowCntSum = rowCnt.reduce(_ + _)

    planner.getCostFactory.makeCost(rowCntSum, cpuCost, ioCost)
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

  private def joinSelectionToString: String = {
    getRowType.getFieldNames.mkString(", ")
  }

  private def joinConditionToString: String = {
    val inFields = resultType.getFieldNames.toList
    getExpressionString(condition, inFields, None)
  }
}

private class FlinkLogicalMultiJoinConverter
  extends ConverterRule(
    classOf[MultiJoin],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalMultiJoinConverter") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: MultiJoin = call.rel(0).asInstanceOf[MultiJoin]

    // check all joins that are inner joins
    if (join.getJoinTypes.exists(_ != JoinRelType.INNER)) {
      return false
    }

    // extract equality predicates
    val systemFields = new util.ArrayList[RelDataTypeField]()
    val joinKeys = new util.ArrayList[util.List[RexNode]]()
    for (i <- 0 until join.getInputs.size()) {
      joinKeys.add(new util.ArrayList())
    }

    val remaining = RelOptUtil.splitJoinCondition(
      systemFields,
      join.getInputs,
      join.getJoinFilter,
      joinKeys,
      null,
      null)

    (remaining.isAlwaysTrue || isValidJoinedTime(join, remaining)) &&
      joinKeys.size() > 0 && joinKeys.forall(_.size() > 0)
  }

  private def isValidJoinedTime(join: MultiJoin, remaining: RexNode): Boolean = {
    remaining match {
      case call: RexCall if call.op == JoinedTimeSqlFunction.INSTANCE =>
        // check number of arguments
        if (call.getOperands.size() < join.getInputs.size()) {
          throw new ValidationException(
            "Joined time must consist of a time indicator of every table.")
        }
        // proctime
        else if (call.getOperands.forall(op => isProctimeIndicatorType(op.getType))) {
          true
        }
        // rowtime
        else if (call.getOperands.forall(op => isRowtimeIndicatorType(op.getType))) {
          true
        }
        // invalid
        else {
          throw new ValidationException(
            "Joined time must only consists of time indicators of same type.")
        }
      // check for default rowtime
      case _ =>
        if (join.getInputs.forall(_.getRowType.getFieldList
            .exists(f => isRowtimeIndicatorType(f.getType)))) {
          true
        } else {
          throw new ValidationException(
            "Join tables need a common rowtime time indicator.")
        }
    }
  }

  override def convert(rel: RelNode): RelNode = {
    val join = rel.asInstanceOf[MultiJoin]
    val newTraitSet = rel.getTraitSet.replace(FlinkConventions.LOGICAL)
    val newInputs = join.getInputs.map(RelOptRule.convert(_, FlinkConventions.LOGICAL))

    // extract equality predicates
    val systemFields = new util.ArrayList[RelDataTypeField]()
    val joinKeys = new util.ArrayList[util.List[RexNode]]()
    for (i <- 0 until join.getInputs.size()) {
      joinKeys.add(new util.ArrayList())
    }

    val remaining = RelOptUtil.splitJoinCondition(
      systemFields,
      join.getInputs,
      join.getJoinFilter,
      joinKeys,
      null,
      null)

    val isRowtime: Boolean = if (!remaining.isAlwaysTrue && isValidJoinedTime(join, remaining)) {
      val head = remaining.asInstanceOf[RexCall].getOperands.get(0)
      isRowtimeIndicatorType(head.getType)
    } else {
      true // rowtime is the default
    }

    new FlinkLogicalMultiJoin(
      rel.getCluster,
      newTraitSet,
      newInputs,
      join.getJoinFilter,
      joinKeys.map(_.toSeq),
      isRowtime,
      join.getRowType)
  }
}

object FlinkLogicalMultiJoin {
  val CONVERTER: ConverterRule = new FlinkLogicalMultiJoinConverter()
}
