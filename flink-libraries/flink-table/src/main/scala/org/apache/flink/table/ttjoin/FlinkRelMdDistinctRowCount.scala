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

import java.lang.Double

import org.apache.calcite.rel.metadata._
import org.apache.calcite.rex.RexNode
import org.apache.calcite.util.{BuiltInMethod, ImmutableBitSet}
import org.apache.flink.table.plan.nodes.datastream.DataStreamScan

object FlinkRelMdDistinctRowCount extends RelMdDistinctRowCount {

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    BuiltInMethod.DISTINCT_ROW_COUNT.method,
    this)

  def getDistinctRowCount(
    rel: DataStreamScan,
    mq: RelMetadataQuery,
    groupKey: ImmutableBitSet,
    predicate: RexNode): Double = {
    // only return distinct row count for one column
    if (groupKey.cardinality() == 1 && predicate == null) {
      val column = groupKey.nextSetBit(0)
      val columnName = rel.getRowType.getFieldList.get(column).getName
      val columnStats = rel.dataStreamTable.getStatistic.getColumnStats(columnName)
      if (columnStats != null) {
        return columnStats.ndv.toDouble
      }
    }
    null
  }
}
