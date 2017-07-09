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

import org.apache.flink.api.common.state.SortedMapState;
import org.apache.flink.runtime.state.TreeMapNavigator;
import org.apache.flink.util.Navigator;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Simulates a StortedMapState.
 */
public final class AvoidingSortedMapState<K, V> implements SortedMapState<K, V> {

	private final TreeMap<K, V> map;
	private final TreeMapNavigator<K, V> navigator;

	public AvoidingSortedMapState() {
		map = new TreeMap<>();
		navigator = new TreeMapNavigator<>();
		navigator.setMap(map);
	}

	@Override
	public void clear() {
		map.clear();
	}

	@Override
	public V get(K key) throws Exception {
		return map.get(key);
	}

	@Override
	public void put(K key, V value) throws Exception {
		map.put(key, value);
	}

	@Override
	public void putAll(Map<K, V> map) throws Exception {
		this.map.putAll(map);
	}

	@Override
	public void remove(K key) throws Exception {
		map.remove(key);
	}

	@Override
	public boolean contains(K key) throws Exception {
		return map.containsKey(key);
	}

	@Override
	public Iterable<Map.Entry<K, V>> entries() throws Exception {
		return map.entrySet();
	}

	@Override
	public Iterable<K> keys() throws Exception {
		return map.keySet();
	}

	@Override
	public Iterable<V> values() throws Exception {
		return map.values();
	}

	@Override
	public Iterator<Map.Entry<K, V>> iterator() throws Exception {
		return map.entrySet().iterator();
	}

	@Override
	public Navigator<K, V> navigator() throws Exception {
		return navigator;
	}
}
