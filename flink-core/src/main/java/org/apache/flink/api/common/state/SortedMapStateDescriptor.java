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

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.SortedMapSerializer;
import org.apache.flink.api.java.typeutils.SortedMapTypeInfo;

import java.util.SortedMap;

/**
 * A {@link StateDescriptor} for {@link SortedMapState}. This can be used to create state where the type
 * is a sorted map that can be updated and iterated over.
 *
 * <p>Using {@code SortedMapState} is typically more efficient than manually maintaining a tree map in a
 * {@link ValueState}, because the backing implementation can support efficient updates, rather then
 * replacing the full map on write.
 *
 * <p>To create keyed sort state (on a KeyedStream), use
 * {@link org.apache.flink.api.common.functions.RuntimeContext#getSortedMapState(SortedMapStateDescriptor)}.
 *
 * @param <UK> The type of the keys that can be added to the map state.
 */
@PublicEvolving
public class SortedMapStateDescriptor<UK, UV> extends StateDescriptor<SortedMapState<UK, UV>, SortedMap<UK, UV>> {

	/**
	 * Create a new {@code SortedMapStateDescriptor} with the given name and the given type serializers.
	 *
	 * @param name The name of the {@code SortedMapStateDescriptor}.
	 * @param keySerializer The type serializer for the keys in the state.
	 * @param valueSerializer The type serializer for the values in the state.
	 */
	public SortedMapStateDescriptor(String name, TypeSerializer<UK> keySerializer, TypeSerializer<UV> valueSerializer) {
		super(name, new SortedMapSerializer<>(keySerializer, valueSerializer), null);
	}

	/**
	 * Create a new {@code SortedMapStateDescriptor} with the given name and the given type informations.
	 *
	 * @param name The name of the {@code SortedMapStateDescriptor}.
	 * @param keyTypeInfo The type information for the keys in the state.
	 * @param valueTypeInfo The type information for the values in the state.
	 */
	public SortedMapStateDescriptor(String name, TypeInformation<UK> keyTypeInfo, TypeInformation<UV> valueTypeInfo) {
		super(name, new SortedMapTypeInfo<>(keyTypeInfo, valueTypeInfo), null);
	}

	/**
	 * Create a new {@code SortedMapStateDescriptor} with the given name and the given type information.
	 *
	 * <p>If this constructor fails (because it is not possible to describe the type via a class),
	 * consider using the {@link #SortedMapStateDescriptor(String, TypeInformation, TypeInformation)} constructor.
	 *
	 * @param name The name of the {@code SortStateDescriptor}.
	 * @param keyClass The class of the type of keys in the state.
	 * @param valueClass The class of the type of values in the state.
	 */
	public SortedMapStateDescriptor(String name, Class<UK> keyClass, Class<UV> valueClass) {
		super(name, new SortedMapTypeInfo<>(keyClass, valueClass), null);
	}

	@Override
	public SortedMapState<UK, UV> bind(StateBinder stateBinder) throws Exception {
		return stateBinder.createSortedMapState(this);
	}

	@Override
	public Type getType() {
		return Type.MAP;
	}

	/**
	 * Gets the serializer for the keys in the state.
	 *
	 * @return The serializer for the keys in the state.
	 */
	public TypeSerializer<UK> getKeySerializer() {
		final TypeSerializer<SortedMap<UK, UV>> rawSerializer = getSerializer();
		if (!(rawSerializer instanceof SortedMapSerializer)) {
			throw new IllegalStateException("Unexpected serializer type.");
		}

		return ((SortedMapSerializer<UK, UV>) rawSerializer).getKeySerializer();
	}

	/**
	 * Gets the serializer for the values in the state.
	 *
	 * @return The serializer for the values in the state.
	 */
	public TypeSerializer<UV> getValueSerializer() {
		final TypeSerializer<SortedMap<UK, UV>> rawSerializer = getSerializer();
		if (!(rawSerializer instanceof SortedMapSerializer)) {
			throw new IllegalStateException("Unexpected serializer type.");
		}

		return ((SortedMapSerializer<UK, UV>) rawSerializer).getValueSerializer();
	}

	@Override
	public int hashCode() {
		int result = serializer.hashCode();
		result = 31 * result + name.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		SortedMapStateDescriptor<?, ?> that = (SortedMapStateDescriptor<?, ?>) o;
		return serializer.equals(that.serializer) && name.equals(that.name);
	}

	@Override
	public String toString() {
		return "SortedMapStateDescriptor{" +
				"name=" + name +
				", serializer=" + serializer +
				'}';
	}
}

