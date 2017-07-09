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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.state.SortedMapState;
import org.apache.flink.util.Navigator;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Simple wrapper sorted map state that exposes empty state properly as an empty sorted map.
 * 
 * @param <K> The type of keys in the map state.
 * @param <V> The type of values in the map state.
 */
class UserFacingSortedMapState<K, V> implements SortedMapState<K, V> {

	private final SortedMapState<K, V> originalState;

	private final TreeMap<K, V> emptyState = new TreeMap<>();

	UserFacingSortedMapState(SortedMapState<K, V> originalState) {
		this.originalState = originalState;
	}

	// ------------------------------------------------------------------------

	@Override
	public V get(K key) throws Exception {
		return originalState.get(key);
	}

	@Override
	public void put(K key, V value) throws Exception {
		originalState.put(key, value);
	}

	@Override
	public void putAll(Map<K, V> value) throws Exception {
		originalState.putAll(value);
	}

	@Override
	public void clear() {
		originalState.clear();
	}

	@Override
	public void remove(K key) throws Exception {
		originalState.remove(key);
	}

	@Override
	public boolean contains(K key) throws Exception {
		return originalState.contains(key);
	}

	@Override
	public Iterable<Map.Entry<K, V>> entries() throws Exception {
		final Iterable<Map.Entry<K, V>> original = originalState.entries();
		return original != null ? original : emptyState.entrySet();
	}

	@Override
	public Iterable<K> keys() throws Exception {
		final Iterable<K> original = originalState.keys();
		return original != null ? original : emptyState.keySet();
	}

	@Override
	public Iterable<V> values() throws Exception {
		final Iterable<V> original = originalState.values();
		return original != null ? original : emptyState.values();
	}

	@Override
	public Iterator<Map.Entry<K, V>> iterator() throws Exception {
		final Iterator<Map.Entry<K, V>> original = originalState.iterator();
		return original != null ? original : emptyState.entrySet().iterator();
	}

	@Override
	public Navigator<K, V> navigator() throws Exception {
		return originalState.navigator();
	}
}
