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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer;
import org.apache.flink.runtime.state.internal.InternalSortedMapState;
import org.apache.flink.util.Navigator;
import org.apache.flink.api.common.state.SortedMapState;
import org.apache.flink.api.common.state.SortedMapStateDescriptor;
import org.apache.flink.runtime.state.TreeMapNavigator;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Heap-backed partitioned {@link SortedMapState} that is snapshotted into files.
 *
 * @param <K>  The type of the key.
 * @param <N>  The type of the namespace.
 * @param <UK> The type of the keys in the state.
 * @param <UV> The type of the values in the state.
 */
public class HeapSortedMapState<K, N, UK, UV>
		extends AbstractHeapState<K, N, TreeMap<UK, UV>, SortedMapState<UK, UV>, SortedMapStateDescriptor<UK, UV>>
		implements InternalSortedMapState<N, UK, UV> {

	private TreeMapNavigator<UK, UV> reusableNavigator;

	/**
	 * Creates a new key/value state for the given tree map of key/value pairs.
	 *
	 * @param stateDesc  The state identifier for the state. This contains name
	 *                   and can create a default state value.
	 * @param stateTable The state table to use in this kev/value state. May contain initial state.
	 */
	public HeapSortedMapState(
			SortedMapStateDescriptor<UK, UV> stateDesc,
			StateTable<K, N, TreeMap<UK, UV>> stateTable,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer) {
		super(stateDesc, stateTable, keySerializer, namespaceSerializer);

		reusableNavigator = new TreeMapNavigator<>();
	}

	@Override
	public UV get(UK userKey) {

		final TreeMap<UK, UV> userMap = stateTable.get(currentNamespace);

		if (userMap == null) {
			return null;
		}

		return userMap.get(userKey);
	}

	@Override
	public void put(UK userKey, UV userValue) {

		TreeMap<UK, UV> userMap = stateTable.get(currentNamespace);
		if (userMap == null) {
			userMap = new TreeMap<>();
			stateTable.put(currentNamespace, userMap);
		}

		userMap.put(userKey, userValue);
	}

	@Override
	public void putAll(Map<UK, UV> value) {

		TreeMap<UK, UV> userMap = stateTable.get(currentNamespace);

		if (userMap == null) {
			userMap = new TreeMap<>();
			stateTable.put(currentNamespace, userMap);
		}

		userMap.putAll(value);
	}

	@Override
	public void remove(UK userKey) {

		final TreeMap<UK, UV> userMap = stateTable.get(currentNamespace);
		if (userMap == null) {
			return;
		}

		userMap.remove(userKey);

		if (userMap.isEmpty()) {
			clear();
		}
	}

	@Override
	public boolean contains(UK userKey) {
		final TreeMap<UK, UV> userMap = stateTable.get(currentNamespace);
		return userMap != null && userMap.containsKey(userKey);
	}

	@Override
	public Iterable<Map.Entry<UK, UV>> entries() {
		final TreeMap<UK, UV> userMap = stateTable.get(currentNamespace);
		return userMap == null ? null : userMap.entrySet();
	}

	@Override
	public Iterable<UK> keys() {
		final TreeMap<UK, UV> userMap = stateTable.get(currentNamespace);
		return userMap == null ? null : userMap.keySet();
	}

	@Override
	public Iterable<UV> values() {
		final TreeMap<UK, UV> userMap = stateTable.get(currentNamespace);
		return userMap == null ? null : userMap.values();
	}

	@Override
	public Iterator<Map.Entry<UK, UV>> iterator() {
		final TreeMap<UK, UV> userMap = stateTable.get(currentNamespace);
		return userMap == null ? null : userMap.entrySet().iterator();
	}

	@Override
	public Navigator<UK, UV> navigator() {
		final TreeMap<UK, UV> userMap = stateTable.get(currentNamespace);
		reusableNavigator.setMap(userMap); // can be null
		return reusableNavigator;
	}

	@Override
	public byte[] getSerializedValue(K key, N namespace) throws IOException {
		Preconditions.checkState(namespace != null, "No namespace given.");
		Preconditions.checkState(key != null, "No key given.");

		final TreeMap<UK, UV> result = stateTable.get(key, namespace);

		if (null == result) {
			return null;
		}

		final TypeSerializer<UK> userKeySerializer = stateDesc.getKeySerializer();
		final TypeSerializer<UV> userValueSerializer = stateDesc.getValueSerializer();

		return KvStateRequestSerializer.serializeMap(result.entrySet(), userKeySerializer, userValueSerializer);
	}
}

