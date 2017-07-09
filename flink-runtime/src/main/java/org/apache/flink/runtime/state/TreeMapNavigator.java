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

import org.apache.flink.util.Navigator;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.TreeMap;

public class TreeMapNavigator<UK, UV> implements Navigator<UK, UV> {

	private TreeMap<UK, UV> map;
	private Map.Entry<UK, UV> currentEntry;
	private Map.Entry<UK, UV> prevEntry;

	public void setMap(final TreeMap<UK, UV> map) {
		this.map = map;
		this.currentEntry = null;
		this.prevEntry = null;
	}

	// access TreeMap internals

	private static final MethodHandle getFirstEntryHandle = accessGetFirstEntryHandle();
	private static final MethodHandle getCeilingEntryHandle = accessGetCeilingEntryHandle();
	private static final MethodHandle successorHandle = accessSuccessorHandle();
	private static final MethodHandle predecessorHandle = accessPredecessorHandle();

	private static MethodHandle accessGetFirstEntryHandle() {
		try {
			final MethodHandles.Lookup lookup = MethodHandles.lookup();
			final Method m = TreeMap.class.getDeclaredMethod("getFirstEntry");
			m.setAccessible(true);
			return lookup.unreflect(m).asType(MethodType.methodType(Map.Entry.class, TreeMap.class));
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}
	}

	private static MethodHandle accessGetCeilingEntryHandle() {
		try {
			final MethodHandles.Lookup lookup = MethodHandles.lookup();
			final Method m = TreeMap.class.getDeclaredMethod("getCeilingEntry", Object.class);
			m.setAccessible(true);
			return lookup.unreflect(m).asType(MethodType.methodType(Map.Entry.class, TreeMap.class, Object.class));
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}
	}

	private static MethodHandle accessSuccessorHandle() {
		try {
			final MethodHandles.Lookup lookup = MethodHandles.lookup();
			final Class clazz = Class.forName("java.util.TreeMap$Entry");
			final Method m = TreeMap.class.getDeclaredMethod("successor", clazz);
			m.setAccessible(true);
			return lookup.unreflect(m).asType(MethodType.methodType(Map.Entry.class, Map.Entry.class));
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}
	}

	private static MethodHandle accessPredecessorHandle() {
		try {
			final MethodHandles.Lookup lookup = MethodHandles.lookup();
			final Class clazz = Class.forName("java.util.TreeMap$Entry");
			final Method m = TreeMap.class.getDeclaredMethod("predecessor", clazz);
			m.setAccessible(true);
			return lookup.unreflect(m).asType(MethodType.methodType(Map.Entry.class, Map.Entry.class));
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void seekToFirst() {
		if (map == null) {
			return;
		}
		prevEntry = null;
		try {
			currentEntry = (Map.Entry<UK, UV>) getFirstEntryHandle.invokeExact(map);
		} catch (Throwable throwable) {
			throw new RuntimeException(throwable);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void seek(UK key) throws Exception {
		if (map == null) {
			return;
		}
		prevEntry = null;
		try {
			currentEntry = (Map.Entry<UK, UV>) getCeilingEntryHandle.invokeExact(map, key);
		} catch (Throwable throwable) {
			throw new RuntimeException(throwable);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void next() {
		if (currentEntry != null) {
			prevEntry = currentEntry;
			try {
				currentEntry = (Map.Entry<UK, UV>) successorHandle.invokeExact(currentEntry);
			} catch (Throwable throwable) {
				throw new RuntimeException(throwable);
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void prev() {
		if (currentEntry != null) {
			try {
				currentEntry = (Map.Entry<UK, UV>) predecessorHandle.invokeExact(currentEntry);
			} catch (Throwable throwable) {
				throw new RuntimeException(throwable);
			}
		} else {
			currentEntry = prevEntry;
		}
		prevEntry = null;
	}

	@Override
	public UK key() {
		if (currentEntry == null) {
			return null;
		}
		return currentEntry.getKey();
	}

	@Override
	public UV value() {
		if (currentEntry == null) {
			return null;
		}
		return currentEntry.getValue();
	}
}
