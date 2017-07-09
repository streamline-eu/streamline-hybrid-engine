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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * General utilities for join.
 */
public final class Utils {

	public static long[] intToLongArray(final int[] array) {
		final long[] newArray = new long[array.length];
		for (int i = 0; i < array.length; i++) {
			newArray[i] = array[i];
		}
		return newArray;
	}

	public static <T> void addMapListEntry(final Map<Integer, List<T>> map, final Integer key, final T value) {
		if (!map.containsKey(key)) {
			final List<T> newList = new ArrayList<>();
			newList.add(value);
			map.put(key, newList);
		} else {
			map.get(key).add(value);
		}
	}

	public static int[] intListToArray(final List<Integer> list) {
		final int[] array = new int[list.size()];
		for (int i = 0; i < list.size(); ++i) {
			array[i] = list.get(i);
		}
		return array;
	}
}
