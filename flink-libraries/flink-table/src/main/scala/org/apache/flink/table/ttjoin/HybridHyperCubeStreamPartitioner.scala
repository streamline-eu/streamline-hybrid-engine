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

import org.apache.flink.runtime.plugable.SerializationDelegate
import org.apache.flink.streaming.runtime.partitioner.{ConfigurableStreamPartitioner, StreamPartitioner}
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord

class HybridHyperCubeStreamPartitioner(val maxParallelism: Int)
  extends StreamPartitioner[JoinRecord]
  with ConfigurableStreamPartitioner {

  val recordChannelArrayPool = new Array[Array[Int]](maxParallelism)

  {
    var i = 0
    while (i < maxParallelism) {
      recordChannelArrayPool(i) = new Array[Int](i)
      i += 1
    }
  }

  val operatorIndices = new Array[Boolean](maxParallelism)

  override def copy(): StreamPartitioner[JoinRecord] = this

  override def configure(maxParallelism: Int): Unit = {
    if (this.maxParallelism != maxParallelism) {
      throw new RuntimeException("Unexpected change of parallelism.")
    }
  }

  override def selectChannels(
      record: SerializationDelegate[StreamRecord[JoinRecord]],
      numChannels: Int): Array[Int] = {

    val joinRecord = record.getInstance().getValue

    val cells = joinRecord.cells

    // determine operator index per cube cell
    util.Arrays.fill(operatorIndices, 0, numChannels, false)
    var operatorIndicesCount = 0
    var i = 0
    while (i < cells.length) {
      val operatorIndex = cells(i) * numChannels / maxParallelism // from key group assigner
      if (!operatorIndices(operatorIndex)) {
        operatorIndices(operatorIndex) = true
        operatorIndicesCount += 1
      }
      i += 1
    }

    val channelArray = recordChannelArrayPool(operatorIndicesCount)

    // send record only once per operator index
    i = 0
    while (operatorIndicesCount > 0) {
      if (operatorIndices(i)) {
        operatorIndicesCount -= 1
        channelArray(operatorIndicesCount) = i
      }
      i += 1
    }

    channelArray
  }

  override def toString: String = "HYBRIDHYPERCUBE"
}
