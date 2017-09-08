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

package org.apache.flink.table.ttjoin;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.SortedMapState;
import org.apache.flink.api.common.state.SortedMapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.ttjoin.ByteKey.FixedByteKeySerializer;
import org.apache.flink.table.ttjoin.ByteKey.VariableByteKeySerializer;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Navigator;

import com.google.common.primitives.Ints;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.api.java.typeutils.runtime.NullMaskUtils.readIntoNullMask;

/**
 * Join operator for a HyperCubeCell.
 */
public final class CubeCellJoin {

	// overallKey = key in data structure representing record (total order key, value id)
	// singleKey = one total order key, e.g. in (a, b, c) it could be b or a
	// totalOrderKey = in (a, b, c) it could be (a, c) for table X
	// prefixKey = in (a, b, c) it could be (a) for table X with (a, c)

	// constants
	private static final byte NULL = (byte) 1;
	private static final byte NOT_NULL = (byte) 0;
	private static final byte ZERO = (byte) 0;

	// state
	private final ValueState<Long> dictionaryCounter; // for variable-length single keys and overall values
	private final MapState<Long, byte[]> dictionaryKeyState; // value id -> value
	private final MapState<ByteKey, byte[]> dictionaryValueState; // value -> (value id, count)
	private final ValueState<Long> orderCounter; // for unique order in case of equal timestamps
	private final SortedMapState<ByteKey, byte[]>[] timedState; // (timestamp, order id) -> (total order key, value id, insert/delete)
	private final SortedMapState<ByteKey, Long>[] structureState; // (total order key, value id) -> count
	private final Navigator<ByteKey, ?>[] joinNavigators; // contains structure state navigators but one delta state navigator

	// current delta state
	private ValueState<Boolean>[] deltaStateEmpty; // number of stored entries in delta states
	private SortedMapState<ByteKey, Boolean>[] deltaState; // (total order key, value id, order id) -> insert/delete
	private ValueState<Boolean> allDeltaStateEmpty; // marks all tables as empty or filled with at least one entry
	private final ValueState<Boolean> allDeltaStateEmptyBlack;
	private final ValueState<Boolean>[] deltaStateEmptyBlack;
	private final SortedMapState<ByteKey, Boolean>[] deltaStateBlack;
	private final ValueState<Boolean> allDeltaStateEmptyWhite;
	private final ValueState<Boolean>[] deltaStateEmptyWhite;
	private final SortedMapState<ByteKey, Boolean>[] deltaStateWhite;

	// config
	private final boolean timedBuffering; // records are buffered according to their timestamp
	private final boolean batchBuffering; // records are buffered for batching joins
	private final TypeSerializer<?>[][] tableFieldSerializers; // field serializers per table

	// buffers
	private final ByteKey lookupKey;
	private final ByteStore deserializationStore; // deserialization buffer
	private final List<byte[]> timedStateDeletions; // buffer for deleted timedState entries

	// inter cube cell information
	private int tableIdx; // current table
	private boolean isInsert; // flag for insert/delete
	private final byte[] serializedProgress; // normalized timestamp for timedBuffer with empty order id
	private final boolean[][] tableNullMasks; // per table, null mask per table
	private final byte[][] tableOverallKey; // per table, with gaps for value ids of variable-length fields
	private final byte[][] variableLengthSingleKeys;
	private final ByteStore variableLengthStore; // variable-length buffer
	private byte[] values; // for overall values

	// join information
	private boolean isSingleRecordEnd; // if variable-length key is not in the dictionary and only one record is processed
	private boolean isJoinEnd; // join operation completed
	private int currentTotalOrderKey; // current single key e.g. (a, b, c, d) could be c
	private final int[][] totalOrderKeyTables; // key a is needed for table R, S, T (sorted by current single key)
	private final byte[][] tableCurrentPrefixKey; // per table (only valid for current total order key tables)
	private final byte[][] currentPrefixKeySorter; // per table (only valid for current total order key tables)
	private final int[] currentSeekIndex; // per current total order key, table where search is taking place
	private int currentDeltaIndex;
	private final int[] prefixKeyMultiplicity; // counters used to backtrack to the beginning of a match

	// result
	private final Collector<CRow> collector;
	private final CRow changeRow;
	private final Row outputRow;
	private final boolean[][] deserializationNullMasks;

	// pre-calculated schema information
	private final int totalKeyOrderArity;
	private final int[][] recordOrderKeyFixedLengths; // per table, keys in record with fixed-lengths, -1 if variable-length
	private final int[][] recordVariableLengthOrderKeyPositions; // per table, position of keys in record with variable-length
	private final int[][] recordVariableLengthOrderKeyOffsets; // per table, offset of order keys (in record with variable-length) in total order key
	private final int[] tableTotalOrderKeyLengths; // per table, length of all single keys (variable-length keys = 8 bytes)
	private final int[] tableOverallKeyLengths; // per table, length of all single keys (variable-length keys = 8 bytes) + value id
	private final int[][] tableSingleKeyOffsets; // per table, offsets to the start of single keys in an overall key for table R
	private final int[] singleKeyLengths; // length of a single key in a total order key (variable-length keys = 8 bytes)
	private final int[][] outputMap; // per table, maps order key to output row

	@SuppressWarnings("unchecked")
	public CubeCellJoin(
		final RuntimeContext context,
		final Collector<CRow> collector,
		final CRow changeRow,
		final int tableCount,
		final TypeSerializer<?>[][] tableFieldSerializers,
		final boolean timedBuffering,
		final boolean batchBuffering,
		final int[] totalKeyOrder,
		final int[][] tableOrderKeyKeySetMap,
		final int[][] outputMap) {

		// config
		this.tableFieldSerializers = tableFieldSerializers;
		this.timedBuffering = timedBuffering;
		this.batchBuffering = batchBuffering;

		// pre-calculated schema information

		this.totalKeyOrderArity = totalKeyOrder.length;

		// calculate lengths for non-variable-lengths keys
		this.recordOrderKeyFixedLengths = new int[tableCount][];
		for (int tableIdx = 0; tableIdx < tableCount; ++tableIdx) {
			this.recordOrderKeyFixedLengths[tableIdx] = new int[tableOrderKeyKeySetMap[tableIdx].length];
			for (int orderKey = 0; orderKey < tableOrderKeyKeySetMap[tableIdx].length; ++orderKey) {
				this.recordOrderKeyFixedLengths[tableIdx][orderKey] = tableFieldSerializers[tableIdx][orderKey].getLength();
			}
		}

		// calculate position of order keys (in record with variable-length)
		// calculate offset of order keys (in record with variable-length) in total order key
		this.recordVariableLengthOrderKeyPositions = new int[tableCount][];
		this.recordVariableLengthOrderKeyOffsets = new int[tableCount][];
		for (int tableIdx = 0; tableIdx < tableCount; ++tableIdx) {
			int variableLengthCount = 0;
			for (int orderKey = 0; orderKey < this.recordOrderKeyFixedLengths[tableIdx].length; ++orderKey) {
				final int length = this.recordOrderKeyFixedLengths[tableIdx][orderKey];
				if (length < 0) {
					variableLengthCount++;
				}
			}
			this.recordVariableLengthOrderKeyPositions[tableIdx] = new int[variableLengthCount];
			this.recordVariableLengthOrderKeyOffsets[tableIdx] = new int[variableLengthCount];
			int variableLengthIndex = 0;
			int offset = 0;
			for (int orderKey = 0; orderKey < this.recordOrderKeyFixedLengths[tableIdx].length; ++orderKey) {
				final int length = this.recordOrderKeyFixedLengths[tableIdx][orderKey];
				if (length < 0) {
					this.recordVariableLengthOrderKeyPositions[tableIdx][variableLengthIndex] = orderKey;
					this.recordVariableLengthOrderKeyOffsets[tableIdx][variableLengthIndex] = offset;
					variableLengthIndex++;
					offset += 8; // variable-length
				} else {
					offset += length; // fixed-length
				}
				offset += 1; // for null
			}
		}

		// calculate total order key lengths
		this.tableTotalOrderKeyLengths = new int[tableCount];
		for (int tableIdx = 0; tableIdx < tableCount; ++tableIdx) {
			int totalOrderKeyLength = 0;
			for (int orderKey = 0; orderKey < this.recordOrderKeyFixedLengths[tableIdx].length; ++orderKey) {
				final int length = this.recordOrderKeyFixedLengths[tableIdx][orderKey];
				if (length > 0) {
					totalOrderKeyLength += length; // fixed-length
				} else {
					totalOrderKeyLength += 8; // variable-length
				}
				totalOrderKeyLength += 1; // for null
			}
			this.tableTotalOrderKeyLengths[tableIdx] = totalOrderKeyLength;
		}

		// calculate overall key lengths
		this.tableOverallKeyLengths = new int[tableCount];
		for (int tableIdx = 0; tableIdx < tableCount; ++tableIdx) {
			this.tableOverallKeyLengths[tableIdx] = this.tableTotalOrderKeyLengths[tableIdx] + 8;
		}

		// calculate offsets of single keys in total order key
		this.tableSingleKeyOffsets = new int[tableCount][];
		for (int tableIdx = 0; tableIdx < tableCount; ++tableIdx) {
			this.tableSingleKeyOffsets[tableIdx] = new int[totalKeyOrder.length];
			// fill with -1 to indicate that table does not contain this single key
			Arrays.fill(this.tableSingleKeyOffsets[tableIdx], -1);
			int totalOrderKeyOffset = 0;
			for (int orderKey = 0; orderKey < this.recordOrderKeyFixedLengths[tableIdx].length; ++orderKey) {
				final int keySet = tableOrderKeyKeySetMap[tableIdx][orderKey];
				final int totalOrderKey = Ints.indexOf(totalKeyOrder, keySet);
				this.tableSingleKeyOffsets[tableIdx][totalOrderKey] = totalOrderKeyOffset;
				final int length = this.recordOrderKeyFixedLengths[tableIdx][orderKey];
				if (length > 0) {
					totalOrderKeyOffset += length; // fixed-length
				} else {
					totalOrderKeyOffset += 8; // variable-length
				}
				totalOrderKeyOffset += 1; // for null
			}
		}

		// calculate single key lengths
		this.singleKeyLengths = new int[totalKeyOrder.length];
		for (int singleKey = 0; singleKey < totalKeyOrder.length; ++singleKey) {
			// find a table that contains single key
			for (int tableIdx = 0; tableIdx < tableCount; ++tableIdx) {
				final int orderKey = Ints.indexOf(tableOrderKeyKeySetMap[tableIdx], singleKey);
				if (orderKey >= 0) {
					int length = this.recordOrderKeyFixedLengths[tableIdx][orderKey];
					if (length < 0) {
						length = 8; // variable-length
					}
					length += 1; // for null
					this.singleKeyLengths[singleKey] = length;
				}
			}
		}

		this.outputMap = outputMap;

		// create join information

		// calculate which key is required by which table
		this.totalOrderKeyTables = new int[totalKeyOrder.length][];
		for (int key = 0; key < totalKeyOrder.length; ++key) {
			final int keySet = totalKeyOrder[key];
			// count tables containing key set
			int tablesWithKeySet = 0;
			for (final int[] keySets : tableOrderKeyKeySetMap) {
				if (Ints.contains(keySets, keySet)) {
					tablesWithKeySet++;
				}
			}
			this.totalOrderKeyTables[key] = new int[tablesWithKeySet];
			// add tables containing key set
			int tables = 0;
			for (int tableIdx = 0; tableIdx < tableCount; ++tableIdx) {
				if (Ints.contains(tableOrderKeyKeySetMap[tableIdx], keySet)) {
					this.totalOrderKeyTables[key][tables] = tableIdx;
					tables++;
				}
			}
		}

		// allocate prefix key per table
		this.tableCurrentPrefixKey = new byte[tableCount][];
		for (int tableIdx = 0; tableIdx < tableCount; ++tableIdx) {
			this.tableCurrentPrefixKey[tableIdx] = new byte[this.tableOverallKeyLengths[tableIdx]];
		}

		this.currentPrefixKeySorter = new byte[tableCount][];

		this.currentSeekIndex = new int[totalKeyOrder.length];

		this.prefixKeyMultiplicity = new int[tableCount];

		// inter cube cell information

		this.serializedProgress = new byte[16];

		this.tableNullMasks = new boolean[tableCount][];
		for (int tableIdx = 0; tableIdx < tableCount; ++tableIdx) {
			this.tableNullMasks[tableIdx] = new boolean[tableFieldSerializers[tableIdx].length];
		}

		this.tableOverallKey = new byte[tableCount][];
		for (int tableIdx = 0; tableIdx < tableCount; ++tableIdx) {
			this.tableOverallKey[tableIdx] = new byte[this.tableOverallKeyLengths[tableIdx]];
		}

		this.variableLengthStore = new ByteStore(ByteStore.DEFAULT_CAPACITY);

		this.variableLengthSingleKeys = new byte[totalKeyOrder.length][];

		// result
		this.collector = collector;
		this.changeRow = changeRow;
		this.outputRow = new Row(outputMap.length);
		this.changeRow.row_$eq(outputRow);

		this.deserializationNullMasks = new boolean[tableCount][];
		for (int tableIdx = 0; tableIdx < tableCount; ++tableIdx) {
			this.deserializationNullMasks[tableIdx] = new boolean[tableFieldSerializers[tableIdx].length];
		}

		// buffer
		this.lookupKey = new ByteKey();
		this.deserializationStore = new ByteStore(ByteStore.DEFAULT_CAPACITY);
		this.timedStateDeletions = new ArrayList<>(32);

		// create state
		dictionaryCounter = context.getState(new ValueStateDescriptor<>("dc", LongSerializer.INSTANCE));
		dictionaryKeyState = context.getMapState(new MapStateDescriptor<>("dk", LongSerializer.INSTANCE, BytePrimitiveArraySerializer.INSTANCE));
		dictionaryValueState = context.getMapState(new MapStateDescriptor<>("dv", VariableByteKeySerializer.INSTANCE, BytePrimitiveArraySerializer.INSTANCE));
		orderCounter = context.getState(new ValueStateDescriptor<>("oc", LongSerializer.INSTANCE));
		if (timedBuffering) {
			timedState = (SortedMapState<ByteKey, byte[]>[]) new SortedMapState[tableCount];
			for (int tableIdx = 0; tableIdx < tableCount; ++tableIdx) {
				timedState[tableIdx] = context.getSortedMapState(new SortedMapStateDescriptor<>("t" + tableIdx, new FixedByteKeySerializer(16), BytePrimitiveArraySerializer.INSTANCE));
			}
		} else {
			timedState = null;
		}

		if (batchBuffering) {
			// prepare black and white states
			allDeltaStateEmptyBlack = context.getState(new ValueStateDescriptor<>("deb", BooleanSerializer.INSTANCE));
			allDeltaStateEmptyWhite = context.getState(new ValueStateDescriptor<>("dew", BooleanSerializer.INSTANCE));
			deltaStateEmptyBlack = (ValueState<Boolean>[]) new ValueState[tableCount];
			deltaStateBlack = (SortedMapState<ByteKey, Boolean>[]) new SortedMapState[tableCount];
			deltaStateEmptyWhite = (ValueState<Boolean>[]) new ValueState[tableCount];
			deltaStateWhite = (SortedMapState<ByteKey, Boolean>[]) new SortedMapState[tableCount];

			// set black as initial
			allDeltaStateEmpty = allDeltaStateEmptyBlack;
			deltaStateEmpty = deltaStateEmptyBlack;
			deltaState = deltaStateBlack;
		} else {
			// set avoiding states
			allDeltaStateEmpty = new AvoidingValueState<>();
			deltaStateEmpty = (ValueState<Boolean>[]) new ValueState[tableCount];
			deltaState = (SortedMapState<ByteKey, Boolean>[]) new SortedMapState[tableCount];

			// set black and white states to null
			allDeltaStateEmptyBlack = null;
			allDeltaStateEmptyWhite = null;
			deltaStateEmptyBlack = null;
			deltaStateBlack = null;
			deltaStateEmptyWhite = null;
			deltaStateWhite = null;
		}

		for (int tableIdx = 0; tableIdx < tableCount; ++tableIdx) {

			// we need to store multiple records in state for later batching
			if (batchBuffering) {
				// prepare black and white states
				deltaStateEmptyBlack[tableIdx] = context.getState(new ValueStateDescriptor<>("dcb" + tableIdx, BooleanSerializer.INSTANCE));
				deltaStateBlack[tableIdx] = context.getSortedMapState(
					new SortedMapStateDescriptor<>("db" + tableIdx, new FixedByteKeySerializer(tableTotalOrderKeyLengths[tableIdx] + 16), BooleanSerializer.INSTANCE));
				deltaStateEmptyWhite[tableIdx] = context.getState(new ValueStateDescriptor<>("dcw" + tableIdx, BooleanSerializer.INSTANCE));
				deltaStateWhite[tableIdx] = context.getSortedMapState(
					new SortedMapStateDescriptor<>("dw" + tableIdx, new FixedByteKeySerializer(tableTotalOrderKeyLengths[tableIdx] + 16), BooleanSerializer.INSTANCE));
			}
			// only watermark batching or no batching is happening
			else {
				// set avoiding states
				deltaStateEmpty[tableIdx] = new AvoidingValueState<>();
				deltaState[tableIdx] = new AvoidingSortedMapState<>();
			}
		}

		structureState = (SortedMapState<ByteKey, Long>[]) new SortedMapState[tableCount];
		for (int tableIdx = 0; tableIdx < tableCount; ++tableIdx) {
			structureState[tableIdx] = context.getSortedMapState(
				new SortedMapStateDescriptor<>("s" + tableIdx, new FixedByteKeySerializer(tableTotalOrderKeyLengths[tableIdx] + 8), LongSerializer.INSTANCE));
		}
		joinNavigators = (Navigator<ByteKey, ?>[]) new Navigator[tableCount];
	}

	public final void insertRecord(
			final byte tableIdx,
			final boolean isInsert,
			final long timestamp,
			final ByteStore serializedRow) throws IOException {

		// cache variables
		final TypeSerializer[] fieldSerializers = tableFieldSerializers[tableIdx];
		final boolean[] nullMask = tableNullMasks[tableIdx];
		final int[] singleKeyFixedLengths = recordOrderKeyFixedLengths[tableIdx];
		final byte[] overallKey = tableOverallKey[tableIdx];

		// store table index
		this.tableIdx = tableIdx;

		// insert/delete
		this.isInsert = isInsert;

		// timestamp
		if (timedBuffering) {
			ByteKey.serializeLong(this.serializedProgress, 0, normalizeTimestamp(timestamp));
		}

		// deserialize null mask
		readIntoNullMask(tableFieldSerializers.length, serializedRow, nullMask);

		// store keys in inter cube cell buffers
		int offset = 0;
		for (int key = 0; key < singleKeyFixedLengths.length; ++key) {
			final int length = singleKeyFixedLengths[key];
			// fixed-length
			if (length > 0) {
				// null
				if (nullMask[key]) {
					overallKey[offset++] = NULL;
					// overwrite a previous value
					Arrays.fill(overallKey, offset, offset += length, ZERO);
				}
				// not null
				else {
					overallKey[offset++] = NOT_NULL;
					offset += serializedRow.read(overallKey, offset, length);
				}
			}
			// variable-length
			else {
				// null
				if (nullMask[key]) {
					overallKey[offset++] = NULL;
					// overwrite a previous value id
					Arrays.fill(overallKey, offset, offset += 8, ZERO);
				} else {
					overallKey[offset++] = NOT_NULL;
					offset += 8; // will be replaced with value id later
					variableLengthStore.reset();
					fieldSerializers[key].copy(serializedRow, variableLengthStore);
					variableLengthSingleKeys[key] = variableLengthStore.toByteArray();
				}
			}
		}

		// store values in inter cube cell buffers
		this.values = serializedRow.toByteArray();
	}

	public final void storeRecord() throws Exception {

		// cache variables
		final int[] variableLengthSingleKeyPositions = recordVariableLengthOrderKeyPositions[tableIdx];
		final int[] variableLengthSingleKeyOffsets = recordVariableLengthOrderKeyOffsets[tableIdx];
		final byte[] overallKey = tableOverallKey[tableIdx];

		// add variable-length keys to dictionary
		for (int keyIndex = 0; keyIndex < variableLengthSingleKeyPositions.length; ++keyIndex) {
			final int pos = variableLengthSingleKeyPositions[keyIndex];
			final int offset = variableLengthSingleKeyOffsets[keyIndex];
			final byte[] singleKey = variableLengthSingleKeys[pos];
			// the dictionary is used to speedup the join, for non-existing variable-length keys
			final boolean impactsJoin = !timedBuffering && !batchBuffering;
			final long valueId = addDictionaryEntry(singleKey, impactsJoin); // modifies lookupKey!
			ByteKey.serializeLong(overallKey, offset, valueId);
		}

		// add values to dictionary
		final long valueId = addDictionaryEntry(values, false); // modifies lookupKey!
		ByteKey.serializeLong(overallKey, overallKey.length - 8, valueId); // value id

		// create unique order id
		final Long nextOrderIdCounter = orderCounter.value();
		final long nextOrderId;
		if (nextOrderIdCounter == null) {
			nextOrderId = 0L;
		} else {
			nextOrderId = nextOrderIdCounter;
		}
		orderCounter.update(nextOrderId + 1);

		// add timed entry
		if (timedBuffering) {
			addTimedEntry(nextOrderId);
		}
		// add delta entry
		else {
			addDeltaEntry(nextOrderId);
		}
	}

	public void switchDelta(final boolean toEmpty) throws Exception {
		// only toggle if delta is empty
		final Boolean emptyBlack = allDeltaStateEmptyBlack.value();
		// we want the empty delta
		if (toEmpty) {
			if (emptyBlack == null || emptyBlack) {
				allDeltaStateEmpty = allDeltaStateEmptyBlack;
				deltaStateEmpty = deltaStateEmptyBlack;
				deltaState = deltaStateBlack;
			} else {
				allDeltaStateEmpty = allDeltaStateEmptyWhite;
				deltaStateEmpty = deltaStateEmptyWhite;
				deltaState = deltaStateWhite;
			}
		}
		// we want the non-empty delta
		else {
			if (emptyBlack != null && !emptyBlack) {
				allDeltaStateEmpty = allDeltaStateEmptyBlack;
				deltaStateEmpty = deltaStateEmptyBlack;
				deltaState = deltaStateBlack;
			} else {
				allDeltaStateEmpty = allDeltaStateEmptyWhite;
				deltaStateEmpty = deltaStateEmptyWhite;
				deltaState = deltaStateWhite;
			}
		}
	}

	private void addTimedEntry(final long orderId) throws Exception {

		// cache variables
		final byte[] overallKey = tableOverallKey[tableIdx];
		final int overallKeyLength = overallKey.length;

		// create key for timed state
		final byte[] timedStateKey = new byte[16]; // (timestamp, order id)
		System.arraycopy(serializedProgress, 0, timedStateKey, 0, 8);
		ByteKey.serializeLong(timedStateKey, 8, orderId);

		// create value for timed state
		final byte[] timedStateValue = new byte[overallKeyLength + 1]; // (total order key, value id, insert/delete)
		System.arraycopy(overallKey, 0, timedStateValue, 0, overallKeyLength); // overall key
		ByteKey.serializeBoolean(timedStateValue, overallKeyLength, isInsert); // insert/delete

		// add entry
		timedState[tableIdx].put(new ByteKey(timedStateKey), timedStateValue); // no reuse because new key
	}

	private void addDeltaEntry(final long orderId) throws Exception {

		// cache variables
		final byte[] overallKey = tableOverallKey[tableIdx];
		final int overallKeyLength = overallKey.length;

		// create key for delta state
		final byte[] deltaStateKey = new byte[overallKeyLength + 16]; // (total order key, value id, order id)
		System.arraycopy(overallKey, 0, deltaStateKey, 0, overallKeyLength); // total order key
		ByteKey.serializeLong(deltaStateKey, overallKeyLength, orderId); // order id

		// add entry
		deltaState[tableIdx].put(new ByteKey(deltaStateKey), isInsert); // no reuse, because new key

		// mark delta
		deltaStateEmpty[tableIdx].update(false);
		allDeltaStateEmpty.update(false);
	}

	// adds an arbitrary value to a dictionary and maintains a reference counter
	// returns dictionary identifier
	private long addDictionaryEntry(final byte[] value, final boolean impactsJoin) throws Exception {
		// lookup in dictionary
		final ByteKey key = lookupKey;
		key.replace(value);
		final byte[] lookup = dictionaryValueState.get(key);

		// new dictionary entry
		if (lookup == null) {
			if (impactsJoin) {
				isSingleRecordEnd = true;
			}

			final Long nextValueIdCounter = dictionaryCounter.value();
			final long nextValueId;
			if (nextValueIdCounter == null) {
				nextValueId = 0L;
			} else {
				nextValueId = nextValueIdCounter;
			}
			dictionaryCounter.update(nextValueId + 1);

			dictionaryKeyState.put(nextValueId, value); // reuse, because either RocksDB copy or existing object will be used

			final byte[] idAndCount = new byte[16];
			ByteKey.serializeLong(idAndCount, 0, nextValueId);
			ByteKey.serializeLong(idAndCount, 8, 1L);

			dictionaryValueState.put(new ByteKey(value), idAndCount); // no reuse, because new key

			return nextValueId;
		}
		// entry exists
		else {
			final long valueId = ByteKey.deserializeLong(lookup, 0);
			final long count = ByteKey.deserializeLong(lookup, 8);
			ByteKey.serializeLong(lookup, 8, count + 1);
			dictionaryValueState.put(key, lookup); // reuse, because either RocksDB copy or existing object will be used

			return valueId;
		}
	}

	public final void join() throws Exception {

		// all delta states are empty, nothing to do
		final Boolean allEmpty = allDeltaStateEmpty.value();
		if (allEmpty == null || allEmpty) {
			return;
		}

		// initialize navigators
		for (int tableIdx = 0; tableIdx < structureState.length; ++tableIdx) {
			// initiate navigators
			joinNavigators[tableIdx] = structureState[tableIdx].navigator();
		}

		// perform a delta join with each non-empty buffer
		for (int i = 0; i < deltaState.length; ++i) {

			final Boolean empty = deltaStateEmpty[i].value();

			if (empty != null && !empty) {

				// restore previous navigator
				if (i > 0) {
					joinNavigators[i - 1] = structureState[i - 1].navigator();
				}

				// set current delta index
				currentDeltaIndex = i;
				joinNavigators[i] = deltaState[i].navigator();

				// join
				if (!isSingleRecordEnd) {
					joinDelta();
				}

				// merge
				applyDelta(i);
			}
		}

		// mark all delta states as empty
		allDeltaStateEmpty.update(true);
	}

	private void joinDelta() throws Exception {

		// reset join information
		isJoinEnd = false;
		currentTotalOrderKey = -1;

		// reset iterators
		for (final Navigator<?, ?> navigator : joinNavigators) {
			navigator.seekToFirst();
		}

		boolean match = nextTotalOrderKey();
		while (!isJoinEnd) {
			// we found a matching key for all tables
			if (match) {
				// all total order keys are matching, emit result
				if (currentTotalOrderKey == totalKeyOrderArity - 1) {
					emitMatch();
					match = nextMatch();
				}
				// current total order key match
				// or a previous total order key did not match
				else {
					match = nextTotalOrderKey();
				}
			} else {
				// reset all prefixes that might have overflown
				// go to previous total order key
				match = previousTotalOrderKey();
			}
		}
	}

	private boolean nextTotalOrderKey() throws Exception {
		// next level
		currentTotalOrderKey += 1;

		// cache variables
		final int[] tables = totalOrderKeyTables[currentTotalOrderKey];
		final int singleKeyLength = singleKeyLengths[currentTotalOrderKey];

		// for all tables that contain total order key
		for (final int table : tables) {

			// current delta is preferred
			final Navigator<ByteKey, ?> navigator = joinNavigators[table];

			// extract overall key at current seek position
			final ByteKey overallKey = navigator.key();

			// navigator has no elements
			if (overallKey == null) {
				isJoinEnd = true;
				return false;
			}

			// cache variables
			final byte[] currentPrefixKey = tableCurrentPrefixKey[table];
			final int singleKeyOffset = tableSingleKeyOffsets[table][currentTotalOrderKey];

			// prefix key includes next single key
			final int filledPrefixLength = singleKeyOffset + singleKeyLength;

			// copy prefix key
			System.arraycopy(overallKey.getData(), 0, currentPrefixKey, 0, filledPrefixLength);

			// fill remaining prefix space with zeros
			Arrays.fill(currentPrefixKey, filledPrefixLength, currentPrefixKey.length, (byte) 0x00);
		}

		// prepare tables and join them
		return prepareJoining();
	}

	private boolean prepareJoining() throws Exception {

		// cache variables
		final int[] tableMapping = totalOrderKeyTables[currentTotalOrderKey];

		// store all prefix keys for all tables of current total order key into key sort buffer
		for (int i = 0; i < tableMapping.length; ++i) {
			// not the entire buffer must be filled
			currentPrefixKeySorter[i] = tableCurrentPrefixKey[tableMapping[i]];
		}

		// sort prefix key sort buffer and table mapping at the same time
		for (int i = 0; i < tableMapping.length; i++) {
			for (int j = 0; j < tableMapping.length; j++) {
				// compare by single key
				final int table = tableMapping[i];
				final int otherTable = tableMapping[j];

				if (compareCurrentSingleKey(table, otherTable) > 0 && i < j){
					final byte[] tempPrefixKey = currentPrefixKeySorter[i];

					tableMapping[i] = otherTable;
					currentPrefixKeySorter[i] = currentPrefixKeySorter[j];

					tableMapping[j] = table;
					currentPrefixKeySorter[j] = tempPrefixKey;
				}
			}
		}

		currentSeekIndex[currentTotalOrderKey] = 0;

		return findMatch();
	}

	private int compareCurrentSingleKey(final int table, final int otherTable) {
		final byte[] prefixKey = tableCurrentPrefixKey[table];
		final byte[] otherPrefixKey = tableCurrentPrefixKey[otherTable];

		final int singleKeyOffset = tableSingleKeyOffsets[table][currentTotalOrderKey];
		final int otherSingleKeyOffset = tableSingleKeyOffsets[otherTable][currentTotalOrderKey];

		final int singleKeyLength = singleKeyLengths[currentTotalOrderKey];

		return ByteKey.compareInRange(
			prefixKey,
			singleKeyOffset,
			singleKeyLength,
			otherPrefixKey,
			otherSingleKeyOffset,
			singleKeyLength);
	}

	private boolean findMatch() throws Exception {

		// cache variables
		final int[] tableMapping = totalOrderKeyTables[currentTotalOrderKey];
		final int tableCount = tableMapping.length;

		// table with maximum single key
		int maxTable = tableMapping[mod(currentSeekIndex[currentTotalOrderKey] - 1, tableCount)];
		while (true) {
			// table with minimum single key
			int minTable = tableMapping[currentSeekIndex[currentTotalOrderKey]];

			// current min single key == current max single key
			if (compareCurrentSingleKey(minTable, maxTable) == 0) {
				return true;
			} else {
				// seek current seek table to total order prefix and single key
				prefixSeek(minTable, maxTable);
				// check if we are at the end of the prefix without single key
				if (prefixEnd(minTable)) {
					return false;
				} else {
					// update the min table prefix
					updatePrefixKey(minTable);
					// after successful seek the updated min table is now the new max table
					maxTable = minTable;
					currentSeekIndex[currentTotalOrderKey] = mod(currentSeekIndex[currentTotalOrderKey] + 1, tableCount);
				}
			}
		}
	}

	// seek to current prefix key of seek table with single key of other table
	private void prefixSeek(int seekTable, int otherTable) throws Exception {

		// cache variables
		final byte[] seekTablePrefixKey = tableCurrentPrefixKey[seekTable];
		final byte[] otherPrefixKey = tableCurrentPrefixKey[otherTable];

		// current total order key in seek table
		final int singleKeyOffset = tableSingleKeyOffsets[seekTable][currentTotalOrderKey];
		final int singleKeyLength = singleKeyLengths[currentTotalOrderKey];

		// current total order key in other table
		final int otherSingleKeyOffset = tableSingleKeyOffsets[otherTable][currentTotalOrderKey];

		// replace single key of seek table
		System.arraycopy(otherPrefixKey, otherSingleKeyOffset, seekTablePrefixKey, singleKeyOffset, singleKeyLength);

		// seek
		final ByteKey key = lookupKey;
		key.replace(seekTablePrefixKey);
		joinNavigators[seekTable].seek(key);
	}

	// check if prefix at single key is maximum
	private boolean prefixEnd(int seekTable) throws Exception {

		// key
		final ByteKey overallKey = joinNavigators[seekTable].key();

		// we are at the very end
		if (overallKey == null) {
			isJoinEnd = true;
			return true;
		}

		final int singleKeyOffset = tableSingleKeyOffsets[seekTable][currentTotalOrderKey];

		// if prefix keys without current single key differ
		// we are at the next total order key
		return ByteKey.compareInRange(overallKey.getData(), 0, singleKeyOffset, tableCurrentPrefixKey[seekTable], 0, singleKeyOffset) > 0;
	}

	// update current prefix from overall key
	private void updatePrefixKey(int seekTable) throws Exception {

		// cache variables
		final int singleKeyOffset = tableSingleKeyOffsets[seekTable][currentTotalOrderKey];
		final int singleKeyLength = singleKeyLengths[currentTotalOrderKey];

		// key
		final byte[] overallKey = joinNavigators[seekTable].key().getData();

		// prefix key includes next single key
		final int filledPrefixLength = singleKeyOffset + singleKeyLength;
		System.arraycopy(overallKey, 0, tableCurrentPrefixKey[seekTable], 0, filledPrefixLength);
	}

	private boolean nextMatch() throws Exception {

		// cache variables
		final int[] tableMapping = totalOrderKeyTables[currentTotalOrderKey];
		final int tableCount = tableMapping.length;

		final int seekTable = tableMapping[currentSeekIndex[currentTotalOrderKey]];
		// seek to next entry within current total order key for min single key
		prefixNext(seekTable);
		if (prefixEnd(seekTable)) {
			return false;
		} else {
			// update the min table prefix
			updatePrefixKey(seekTable);
			currentSeekIndex[currentTotalOrderKey] = mod(currentSeekIndex[currentTotalOrderKey] + 1, tableCount);
		}

		return findMatch();
	}

	private void prefixNext(int seekTable) throws Exception {

		// cache variables
		final byte[] prefixKey = tableCurrentPrefixKey[seekTable];
		final int prefixKeyLength = prefixKey.length;
		final int singleKeyOffset = tableSingleKeyOffsets[seekTable][currentTotalOrderKey];
		final int singleKeyLength = singleKeyLengths[currentTotalOrderKey];

		// set all bits after prefix key without single value. E.g. (001, 111, 111)
		Arrays.fill(prefixKey, singleKeyOffset + singleKeyLength, prefixKeyLength, (byte) 0xFF);

		final Navigator<ByteKey, ?> navigator = joinNavigators[seekTable];

		// seek to prefix key
		final ByteKey key = lookupKey;
		key.replace(prefixKey);
		navigator.seek(key);

		// it might happen that this prefix key exists, but actually we want the next key
		final ByteKey overallKey = navigator.key();
		if (overallKey != null && ByteKey.compareInRange(prefixKey, 0, prefixKeyLength, overallKey.getData(), 0, prefixKeyLength) == 0) {
			navigator.next();
		}
	}

	private boolean previousTotalOrderKey() throws Exception {

		// cache variables
		final int[] tableMapping = totalOrderKeyTables[currentTotalOrderKey];

		for (final int table : tableMapping) {
			final byte[] prefixKey = tableCurrentPrefixKey[table];
			final int prefixKeyLength = prefixKey.length;

			// current total order key in seek table
			final int singleKeyOffset = tableSingleKeyOffsets[table][currentTotalOrderKey];

			Arrays.fill(prefixKey, singleKeyOffset, prefixKeyLength, (byte) 0x00);

			// seek to prefix key
			final ByteKey key = lookupKey;
			key.replace(prefixKey);
			joinNavigators[table].seek(key);
		}

		// previous level
		currentTotalOrderKey -= 1;

		return nextMatch();
	}

	private void applyDelta(final int deltaTable) throws Exception {

		// cache variables
		final SortedMapState<ByteKey, Boolean> delta = deltaState[deltaTable];
		final Navigator<ByteKey, Boolean> deltaNavigator = delta.navigator();
		final SortedMapState<ByteKey, Long> structure = structureState[deltaTable];
		final int overallKeyLength = tableOverallKeyLengths[deltaTable];

		deltaNavigator.seekToFirst();

		ByteKey deltaKey;
		while ((deltaKey = deltaNavigator.key()) != null) {
			// from: (total order key, value id, order id) -> insert/delete
			// to: (total order key, value id) -> count

			// check if overall key exists
			final byte[] overallKey = new byte[overallKeyLength];
			System.arraycopy(deltaKey.getData(), 0, overallKey, 0, overallKeyLength);

			final ByteKey seekKey = lookupKey;
			seekKey.replace(overallKey);
			final Long count = structure.get(seekKey);

			// apply insert/delete
			final boolean isInsert = deltaNavigator.value();

			// insert
			if (isInsert) {
				// new structure entry
				if (count == null) {
					// insert main structure entry
					deltaKey.replace(overallKey); // reuse, because will be serialized by RocksDB or reused from deltaState
					structure.put(deltaKey, 1L);
				}
				// structure entry exists, update counter
				else {
					structure.put(seekKey, count + 1); // reuse, because will be serialized by RocksDB or existing object is used
				}
			}
			// delete
			else {
				// structure entry does not exist
				if (count == null) {
					throw new RuntimeException("Cannot delete entry that does not exist.");
				}
				// delete structure entry
				if (count <= 1) {
					deleteOverallKey(deltaTable, overallKey); // modifies lookupKey!
				}
				// update counter
				else {
					structure.put(seekKey, count - 1); // reuse, because will be serialized by RocksDB or existing object is used
				}
			}

			deltaNavigator.next();
		}

		// update delta counter
		deltaStateEmpty[deltaTable].update(true);

		// update delta state
		delta.clear();
	}

	private void deleteOverallKey(final int tableIdx, final byte[] overallKey) throws Exception {

		// cache variables
		final int[] variableLengthOffsets = this.recordVariableLengthOrderKeyOffsets[tableIdx];

		// delete variable-length single keys
		for (final int singleKeyOffset : variableLengthOffsets) {
			// delete key if not null
			if (overallKey[singleKeyOffset] == NOT_NULL) {
				final long valueId = ByteKey.deserializeLong(overallKey, singleKeyOffset) + 1;
				removeDictionaryEntry(valueId); // modifies lookupKey!
			}
		}

		// delete value reference of overall key
		// value id has no null byte
		final long valueId = ByteKey.deserializeLong(overallKey, overallKey.length - 8);
		removeDictionaryEntry(valueId); // modifies lookupKey!

		// delete entry
		final ByteKey seekKey = lookupKey;
		seekKey.replace(overallKey);
		structureState[tableIdx].remove(seekKey);
	}

	private void removeDictionaryEntry(final long valueId) throws Exception {

		final byte[] value = dictionaryKeyState.get(valueId);

		final ByteKey seekKey = lookupKey;
		seekKey.replace(value);
		final byte[] idAndCount = dictionaryValueState.get(seekKey);

		final long count = ByteKey.deserializeLong(idAndCount, 8);
		// last entry, remove dictionary entry
		if (count <= 1) {
			dictionaryKeyState.remove(valueId);
			dictionaryValueState.remove(seekKey);
		}
		// decrease reference counter
		else {
			ByteKey.serializeLong(idAndCount, 8, count - 1);
			dictionaryValueState.put(seekKey, idAndCount); // reuse, because will be serialized by RocksDB or existing object is used
		}
	}

	public void insertWatermark(final long timestamp) {
		if (timedBuffering) {
			ByteKey.serializeLong(this.serializedProgress, 0, normalizeTimestamp(timestamp));
		}
	}

	public void storeProgress() throws Exception {

		// for each table
		for (int tableIdx = 0; tableIdx < structureState.length; ++tableIdx) {

			// cache variables
			final int overallKeyLength = tableOverallKeyLengths[tableIdx];
			final SortedMapState<ByteKey, byte[]> timed = timedState[tableIdx];
			final Navigator<ByteKey, byte[]> timedNavigator = timedState[tableIdx].navigator();
			final SortedMapState<ByteKey, Boolean> delta = deltaState[tableIdx];
			final List<byte[]> deletions = timedStateDeletions;

			// iterate through timed state and move records to delta
			final ByteKey seekKey = lookupKey;
			seekKey.replace(serializedProgress);
			timedNavigator.seek(seekKey);

			boolean markOnce = false;

			// move records
			ByteKey timedKey;
			while ((timedKey = timedNavigator.key()) != null) {

				// clear buffer once
				if (!markOnce) {
					markOnce = true;

					// mark as non-empty once
					deltaStateEmpty[tableIdx].update(false);
					allDeltaStateEmpty.update(false);

					// clear buffer once
					deletions.clear();
				}

				// add key to buffer for later deletion
				deletions.add(ByteKey.copy(timedKey.getData()));

				final byte[] timedValue = timedNavigator.value();

				// from: (timestamp, order id) -> (total order key, value id, insert/delete)
				// to: (total order key, value id, order id) -> insert/delete

				// create delta key
				final byte[] deltaKey = new byte[overallKeyLength + 8];
				System.arraycopy(timedValue, 0, deltaKey, 0, overallKeyLength); // total order key, value id
				System.arraycopy(timedKey.getData(), 8, deltaKey, overallKeyLength, 8); // order id

				// create delta value
				final boolean deltaValue = ByteKey.deserializeBoolean(timedValue, overallKeyLength);

				// add delta entry
				// no reuse, because timedKey object is reused by serializer in RocksDB and is not serialized with AvoidingState
				delta.put(new ByteKey(deltaKey), deltaValue);

				timedNavigator.next();
			}

			// delete records
			for (final byte[] timedKeyData : deletions) {
				seekKey.replace(timedKeyData);
				timed.remove(seekKey);
			}
		}
	}

	private void emitMatch() throws Exception {

		// count number of matching entries with same prefix key
		for (int i = 0; i < joinNavigators.length; ++i) {

			// cache variables
			final Navigator<ByteKey, ?> navigator = joinNavigators[i];
			final byte[] prefixKey = tableCurrentPrefixKey[i];
			final int prefixKeyLength = prefixKey.length - 8; // ignore value id

			// at least one result is present as we have a match
			prefixKeyMultiplicity[i] = 1;

			navigator.next();

			ByteKey key;
			while ((key = navigator.key()) != null &&
					ByteKey.compareInRange(prefixKey, 0, prefixKeyLength, key.getData(), 0, prefixKeyLength) == 0) {
				prefixKeyMultiplicity[i]++;
				navigator.next();
			}
			// go back to first entry
			for (int j = 0; j < prefixKeyMultiplicity[i]; ++j) {
				navigator.prev();
			}
		}

		// emit the cartesian product of all entries with same total order key but multiple values
		// after this call the iterators are at the end of the prefix key
		emitCartesianProduct(0);
	}

	private void emitCartesianProduct(final int level) throws Exception {

		// cache variables
		final Navigator<ByteKey, ?> navigator = joinNavigators[level];

		for (int i = 0; i < prefixKeyMultiplicity[level]; ++i) {

			// consider count of same overall key
			if (level == currentDeltaIndex) {
				if (level == structureState.length - 1) {
					emitRecord();
				} else {
					// a delta buffer has no count
					emitCartesianProduct(level + 1);
				}
			} else {
				final long overallKeyMultiplicity = (Long) navigator.value();
				for (long j = 0; j < overallKeyMultiplicity; ++j) {
					if (level == structureState.length - 1) {
						emitRecord();
					} else {
						emitCartesianProduct(level + 1);
					}
				}
			}

			// next
			if (i + 1 < prefixKeyMultiplicity[level]) {
				navigator.next();
			}
		}

		// restore navigator position
		for (int i = 0; i < prefixKeyMultiplicity[level] - 1; ++i) {
			navigator.prev();
		}
	}

	private void emitRecord() throws Exception {
		// set insert/delete
		final boolean isInsert = (Boolean) joinNavigators[currentDeltaIndex].value();
		changeRow.change_$eq(isInsert);

		for (int table = 0; table < joinNavigators.length; ++table) {
			final Navigator<ByteKey, ?> navigator = joinNavigators[table];
			final long valueId = ByteKey.deserializeLong(navigator.key().getData(), tableTotalOrderKeyLengths[table]);
			final byte[] value = dictionaryKeyState.get(valueId);
			final TypeSerializer<?>[] fieldSerializers = tableFieldSerializers[table];

			final ByteStore valueStore = deserializationStore;
			valueStore.replace(value);

			readIntoNullMask(fieldSerializers.length, valueStore, deserializationNullMasks[table]);

			for (int field = 0; field < fieldSerializers.length; ++field) {
				final int outputIndex = outputMap[table][field];
				if (!deserializationNullMasks[table][field]) {
					outputRow.setField(outputIndex, fieldSerializers[field].deserialize(valueStore));
				} else {
					outputRow.setField(outputIndex, null);
				}
			}
		}

		collector.collect(changeRow);
	}

	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("NumericOverflow")
	private static long normalizeTimestamp(final long l) {
		// -4 -3 -2 -1 0 1 2 3
		//  7  6  5  4 3 2 1 0
		return (~l) ^ (1L << 63);
	}

	private static int mod(final int a, final int b) {
		return (((a % b) + b) % b);
	}
}
