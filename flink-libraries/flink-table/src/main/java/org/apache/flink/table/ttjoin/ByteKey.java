/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.ttjoin;

import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.ParameterlessTypeSerializerConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Map;

/**
 * Wrapper class for byte array keys.
 */
@TypeInfo(ByteKey.TypeInfoFactory.class)
public final class ByteKey implements Comparable<ByteKey> {

	private byte[] data;

	public ByteKey() {
		this.data = new byte[0];
	}

	public ByteKey(final byte[] data) {
		this.data = data;
	}

	public void replace(final byte[] data) {
		this.data = data;
	}

	public byte[] getData() {
		return data;
	}

	public ByteKey copy() {
		final byte[] dataCopy = new byte[data.length];
		System.arraycopy(data, 0, dataCopy, 0, data.length);
		return new ByteKey(dataCopy);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(data);
	}

	@Override
	@SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
	public boolean equals(final Object o) {
		final byte[] data = this.data;
		final byte[] otherData = ((ByteKey) o).data;
		return Arrays.equals(data, otherData);
	}

	@Override
	public int compareTo(ByteKey o) {

		// cache variables
		final int len1 = this.data.length;
		final int len2 = o.data.length;
		final byte[] b1 = this.data;
		final byte[] b2 = o.data;

		// compare
		final int minLength = Math.min(len1, len2);
		for (int i = 0; i < minLength; i++) {
			int result = (b1[i] & 0xff) -  (b2[i] & 0xff);
			if (result != 0) {
				return result;
			}
		}
		return len1 - len2;
	}

	@Override
	public String toString() {
		return Arrays.toString(data);
	}

	// --------------------------------------------------------------------------------------------

	public static void serializeLong(final byte[] b, final int offset, final long v) {
		b[offset] = (byte) (v >> 56);
		b[offset + 1] = (byte) (v >> 48);
		b[offset + 2] = (byte) (v >> 40);
		b[offset + 3] = (byte) (v >> 32);
		b[offset + 4] = (byte) (v >> 24);
		b[offset + 5] = (byte) (v >> 16);
		b[offset + 6] = (byte) (v >> 8);
		b[offset + 7] = (byte) v;
	}

	public static long deserializeLong(final byte[] b, final int offset) {
		return (b[offset] & 0xFFL) << 56
				| (b[offset + 1] & 0xFFL) << 48
				| (b[offset + 2] & 0xFFL) << 40
				| (b[offset + 3] & 0xFFL) << 32
				| (b[offset + 4] & 0xFFL) << 24
				| (b[offset + 5] & 0xFFL) << 16
				| (b[offset + 6] & 0xFFL) << 8
				| (b[offset + 7] & 0xFFL);
	}

	public static void serializeBoolean(final byte[] b, final int offset, final boolean v) {
		b[offset] = (v) ? (byte) 1 : (byte) 0;
	}

	public static boolean deserializeBoolean(final byte[] buffer, final int offset) {
		return buffer[offset] == (byte) 1;
	}

	public static int compareInRange(
		final byte[] arr1,
		final int from1,
		final int len1,
		final byte[] arr2,
		final int from2,
		final int len2) {

		final int minLength = Math.min(len1, len2);
		for (int i = 0; i < minLength; i++) {
			int result = (arr1[i + from1] & 0xff) -  (arr2[i + from2] & 0xff);
			if (result != 0) {
				return result;
			}
		}
		return len1 - len2;
	}

	public static byte[] copy(final byte[] src) {
		final int len = src.length;
		final byte[] copy = new byte[len];
		System.arraycopy(src, 0, copy, 0, len);
		return copy;
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * TypeInfoFactory for ByteKey.
	 */
	public static class TypeInfoFactory extends org.apache.flink.api.common.typeinfo.TypeInfoFactory {

		@Override
		public TypeInformation createTypeInfo(Type t, Map genericParameters) {
			throw new UnsupportedOperationException("A ByteKey has no type information.");
		}
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * TypeSerializer for a fixed-length ByteKey.
	 */
	public static class FixedByteKeySerializer extends TypeSerializer<ByteKey> {

		private static final long serialVersionUID = 1L;

		private final byte[] empty;

		private final int size;

		private transient ByteKey reusableByteKey;

		public FixedByteKeySerializer(final int size) {
			this.size = size;
			this.empty = new byte[size];
		}

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public TypeSerializer<ByteKey> duplicate() {
			return new FixedByteKeySerializer(size);
		}

		@Override
		public ByteKey createInstance() {
			return new ByteKey(empty);
		}

		@Override
		public ByteKey copy(ByteKey from) {
			final int s = size;
			final byte[] toData = new byte[s];
			System.arraycopy(from.data, 0, toData, 0, s);
			return new ByteKey(toData);
		}

		@Override
		public ByteKey copy(ByteKey from, ByteKey reuse) {
			System.arraycopy(from.data, 0, reuse.data, 0, size);
			return reuse;
		}

		@Override
		public int getLength() {
			return size;
		}

		@Override
		public void serialize(ByteKey record, DataOutputView target) throws IOException {
			target.write(record.data);
		}

		@Override
		public ByteKey deserialize(DataInputView source) throws IOException {

			// instantiate buffer
			if (reusableByteKey == null) {
				reusableByteKey = new ByteKey(new byte[size]);
			}

			final ByteKey reuse = reusableByteKey;
			source.readFully(reuse.data);

			return reuse;
		}

		@Override
		public ByteKey deserialize(ByteKey reuse, DataInputView source) throws IOException {
			source.readFully(reuse.data);
			return reuse;
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			target.write(source, size);
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj instanceof FixedByteKeySerializer;
		}

		@Override
		public int hashCode() {
			return this.getClass().hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof TypeSerializerSingleton) {
				TypeSerializerSingleton<?> other = (TypeSerializerSingleton<?>) obj;

				return other.canEqual(this);
			} else {
				return false;
			}
		}

		@Override
		public TypeSerializerConfigSnapshot snapshotConfiguration() {
			// type serializer singletons should always be parameter-less
			return new ParameterlessTypeSerializerConfig(getSerializationFormatIdentifier());
		}

		@Override
		public CompatibilityResult<ByteKey> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
			if (configSnapshot instanceof ParameterlessTypeSerializerConfig
					&& isCompatibleSerializationFormatIdentifier(
							((ParameterlessTypeSerializerConfig) configSnapshot).getSerializationFormatIdentifier())) {

				return CompatibilityResult.compatible();
			} else {
				return CompatibilityResult.requiresMigration();
			}
		}

		protected boolean isCompatibleSerializationFormatIdentifier(String identifier) {
			return identifier.equals(getSerializationFormatIdentifier());
		}

		private String getSerializationFormatIdentifier() {
			return getClass().getCanonicalName();
		}
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * TypeSerializer for variable-length ByteKey.
	 */
	public static class VariableByteKeySerializer extends TypeSerializer<ByteKey> {

		public static final VariableByteKeySerializer INSTANCE = new VariableByteKeySerializer();

		private static final long serialVersionUID = 1L;

		private static final byte[] EMPTY = new byte[0];

		private transient ByteKey reusableByteKey;

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public TypeSerializer<ByteKey> duplicate() {
			return new VariableByteKeySerializer();
		}

		@Override
		public ByteKey createInstance() {
			return new ByteKey(EMPTY);
		}

		@Override
		public ByteKey copy(ByteKey from) {
			final byte[] fromData = from.data;
			final byte[] toData = new byte[fromData.length];
			System.arraycopy(fromData, 0, toData, 0, fromData.length);
			return new ByteKey(toData);
		}

		@Override
		public ByteKey copy(ByteKey from, ByteKey reuse) {
			final byte[] fromData = from.data;
			final byte[] reuseData = reuse.data;
			final byte[] toData;
			if (reuseData.length == fromData.length) {
				toData = reuseData;
			} else {
				toData = new byte[fromData.length];
			}
			System.arraycopy(fromData, 0, toData, 0, fromData.length);
			reuse.replace(toData);
			return reuse;
		}

		@Override
		public int getLength() {
			return -1;
		}

		@Override
		public void serialize(ByteKey record, DataOutputView target) throws IOException {
			final byte[] recordData = record.data;
			target.writeInt(recordData.length);
			target.write(recordData);
		}

		@Override
		public ByteKey deserialize(DataInputView source) throws IOException {
			final int len = source.readInt();

			// instantiate buffer
			if (reusableByteKey == null) {
				reusableByteKey = new ByteKey(new byte[len]);
			}

			final ByteKey reuse = reusableByteKey;
			final byte[] reuseData = reuse.data;
			if (reuseData.length == len) {
				source.readFully(reuseData);
			} else {
				final byte[] data = new byte[len];
				source.readFully(data);
				reuse.replace(data);
			}

			return reuse;
		}

		@Override
		public ByteKey deserialize(ByteKey reuse, DataInputView source) throws IOException {
			final byte[] reuseData = reuse.data;
			final int len = source.readInt();
			if (reuseData.length == len) {
				source.readFully(reuseData);
			} else {
				final byte[] data = new byte[len];
				source.readFully(data);
				reuse.replace(data);
			}
			return reuse;
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			final int len = source.readInt();
			target.writeInt(len);
			target.write(source, len);
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj instanceof VariableByteKeySerializer;
		}

		@Override
		public int hashCode() {
			return this.getClass().hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof TypeSerializerSingleton) {
				TypeSerializerSingleton<?> other = (TypeSerializerSingleton<?>) obj;

				return other.canEqual(this);
			} else {
				return false;
			}
		}

		@Override
		public TypeSerializerConfigSnapshot snapshotConfiguration() {
			// type serializer singletons should always be parameter-less
			return new ParameterlessTypeSerializerConfig(getSerializationFormatIdentifier());
		}

		@Override
		public CompatibilityResult<ByteKey> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
			if (configSnapshot instanceof ParameterlessTypeSerializerConfig
					&& isCompatibleSerializationFormatIdentifier(
							((ParameterlessTypeSerializerConfig) configSnapshot).getSerializationFormatIdentifier())) {

				return CompatibilityResult.compatible();
			} else {
				return CompatibilityResult.requiresMigration();
			}
		}

		protected boolean isCompatibleSerializationFormatIdentifier(String identifier) {
			return identifier.equals(getSerializationFormatIdentifier());
		}

		private String getSerializationFormatIdentifier() {
			return getClass().getCanonicalName();
		}
	}
}
