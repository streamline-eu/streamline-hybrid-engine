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

import org.apache.flink.api.common.typeutils.{CompatibilityResult, TypeSerializer, TypeSerializerConfigSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.types.Row

class JoinRecordSerializer(val rowSerializer: TypeSerializer[Row])
  extends TypeSerializer[JoinRecord] {

  @transient
  var reusableJoinRecord: JoinRecord = _

  override def isImmutableType: Boolean = false

  override def duplicate(): TypeSerializer[JoinRecord] =
    new JoinRecordSerializer(rowSerializer.duplicate())

  override def createInstance(): JoinRecord =
    new JoinRecord(-1, -1, -1, new ByteStore(ByteStore.DEFAULT_CAPACITY))

  override def copy(from: JoinRecord): JoinRecord = {
    // create instance with capacity
    val to = new JoinRecord(
      from.tableIdx,
      from.typeFlag,
      from.cubeGroupId,
      from.serializedRow.copy())

    to
  }

  override def copy(from: JoinRecord, reuse: JoinRecord): JoinRecord = {

    // copy metadata
    reuse.tableIdx = from.tableIdx
    reuse.typeFlag = from.typeFlag
    reuse.cubeGroupId = from.cubeGroupId

    // copy payload
    from.serializedRow.copy(reuse.serializedRow)

    reuse
  }

  override def getLength: Int = -1

  override def serialize(record: JoinRecord, target: DataOutputView): Unit = {

    // write metadata
    target.write(record.tableIdx)
    target.write(record.typeFlag)

    // write payload

    // instantiate buffer
    if (reusableJoinRecord == null) {
      reusableJoinRecord = new JoinRecord(-1, -1, -1, new ByteStore(ByteStore.DEFAULT_CAPACITY))
    }

    // write cube group id
    target.writeInt(record.cubeGroupId)

    val reusableSerializedRow = reusableJoinRecord.serializedRow

    // reset buffer
    reusableSerializedRow.reset()

    // fill buffer
    rowSerializer.serialize(record.row, reusableSerializedRow)

    // write buffer
    target.writeInt(reusableSerializedRow.getContent)
    target.write(reusableSerializedRow, reusableSerializedRow.getContent)
  }

  override def deserialize(source: DataInputView): JoinRecord = {

    // instantiate buffer
    if (reusableJoinRecord == null) {
      reusableJoinRecord = new JoinRecord(-1, -1, -1, new ByteStore(ByteStore.DEFAULT_CAPACITY))
    }

    val reuse = this.reusableJoinRecord

    // read metadata
    reuse.tableIdx = source.readByte()
    reuse.typeFlag = source.readByte()

    // read payload

    // read cube group id
    reuse.cubeGroupId = source.readInt()

    val reusableSerializedRow = reuse.serializedRow

    // reset buffer
    reusableSerializedRow.reset()

    // read into buffer
    val size = source.readInt()
    reusableSerializedRow.write(source, size)

    reuse
  }

  override def deserialize(reuse: JoinRecord, source: DataInputView): JoinRecord = {

    // read metadata
    reuse.tableIdx = source.readByte()
    reuse.typeFlag = source.readByte()

    // read payload

    // read cube group id
    reuse.cubeGroupId = source.readInt()

    val reusableSerializedRow = reuse.serializedRow

    // reset buffer
    reusableSerializedRow.reset()

    // read into buffer
    val size = source.readInt()
    reusableSerializedRow.write(source, size)

    reuse
  }

  override def copy(source: DataInputView, target: DataOutputView): Unit = {

    // copy metadata
    target.write(source, 1)
    val typeFlag = source.readByte()
    target.write(typeFlag)

    // copy payload

    // copy cube group id
    target.write(source, 4)

    // copy buffers
    val size = source.readInt()
    target.write(source, size)
  }

  override def canEqual(obj: scala.Any): Boolean = obj.isInstanceOf[JoinRecordSerializer]

  override def snapshotConfiguration(): TypeSerializerConfigSnapshot =
    rowSerializer.snapshotConfiguration()

  override def ensureCompatibility(configSnapshot: TypeSerializerConfigSnapshot)
      : CompatibilityResult[JoinRecord] = CompatibilityResult.compatible()

  override def equals(other: Any): Boolean = other match {
    case that: JoinRecordSerializer =>
      (that canEqual this) &&
        rowSerializer == that.rowSerializer
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(rowSerializer)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
