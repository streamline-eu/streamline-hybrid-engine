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

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemoryUtils;

import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.ByteOrder;

/**
 * Storage for variable-length byte data.
 */
public final class ByteStore implements DataOutputView, DataInputView {

	public static final int DEFAULT_CAPACITY = 512;

	private byte[] buffer;
	private int content;
	private int position;

	public ByteStore(final int size) {
		buffer = new byte[size];
	}

	public void replace(final byte[] buffer) {
		this.buffer = buffer;
		this.content = buffer.length;
		this.position = 0;
	}

	public void reset() {
		this.content = 0;
		this.position = 0;
	}

	public byte[] getBuffer() {
		return this.buffer;
	}

	public int getContent() {
		return this.content;
	}

	public int getPosition() {
		return this.position;
	}

	public byte[] toByteArray() {
		final byte[] copy = new byte[this.content];
		System.arraycopy(this.buffer, 0, copy, 0, this.content);
		return copy;
	}

	public ByteStore copy() {
		final ByteStore newInstance = new ByteStore(this.content);
		System.arraycopy(this.buffer, 0, newInstance.buffer, 0, this.content);
		newInstance.content = this.content;
		return newInstance;
	}

	public void copy(final ByteStore reuse) throws IOException {
		final int len = reuse.content;
		if (this.content >= this.buffer.length - len) {
			resize(len);
		}
		System.arraycopy(this.buffer, 0, reuse.buffer, 0, this.content);
		reuse.content = this.content;
		reuse.position = 0;
	}

	private void resize(int minCapacityAdd) throws IOException {
		int newLen = Math.max(this.buffer.length * 2, this.buffer.length + minCapacityAdd);
		byte[] nb;
		try {
			nb = new byte[newLen];
		}
		catch (NegativeArraySizeException e) {
			throw new IOException("Serialization failed because the record length would exceed 2GB (max addressable array size in Java).");
		}
		catch (OutOfMemoryError e) {
			// this was too large to allocate, try the smaller size (if possible)
			if (newLen > this.buffer.length + minCapacityAdd) {
				newLen = this.buffer.length + minCapacityAdd;
				try {
					nb = new byte[newLen];
				}
				catch (OutOfMemoryError ee) {
					// still not possible. give an informative exception message that reports the size
					throw new IOException("Failed to serialize element. Serialized size (> "
							+ newLen + " bytes) exceeds JVM heap space", ee);
				}
			} else {
				throw new IOException("Failed to serialize element. Serialized size (> "
						+ newLen + " bytes) exceeds JVM heap space", e);
			}
		}

		System.arraycopy(this.buffer, 0, nb, 0, this.content);
		this.buffer = nb;
	}

	// ----------------------------------------------------------------------------------------
	//                               Data Output
	// ----------------------------------------------------------------------------------------

	@Override
	public void write(final int b) throws IOException {
		if (this.content >= this.buffer.length) {
			resize(1);
		}
		this.buffer[this.content++] = (byte) (b & 0xff);
	}

	@Override
	public void write(final byte[] b) throws IOException {
		write(b, 0, b.length);
	}

	@Override
	public void write(final byte[] b, final int off, final int len) throws IOException {
		if (len < 0 || off > b.length - len) {
			throw new ArrayIndexOutOfBoundsException();
		}
		if (this.content > this.buffer.length - len) {
			resize(len);
		}
		System.arraycopy(b, off, this.buffer, this.content, len);
		this.content += len;
	}

	@Override
	public void writeBoolean(final boolean v) throws IOException {
		write(v ? 1 : 0);
	}

	@Override
	public void writeByte(final int v) throws IOException {
		write(v);
	}

	@Override
	public void writeBytes(final String s) throws IOException {
		final int sLen = s.length();
		if (this.content >= this.buffer.length - sLen) {
			resize(sLen);
		}

		for (int i = 0; i < sLen; i++) {
			writeByte(s.charAt(i));
		}
		this.content += sLen;
	}

	@Override
	public void writeChar(final int v) throws IOException {
		if (this.content >= this.buffer.length - 1) {
			resize(2);
		}
		this.buffer[this.content++] = (byte) (v >> 8);
		this.buffer[this.content++] = (byte) v;
	}

	@Override
	public void writeChars(final String s) throws IOException {
		final int sLen = s.length();
		if (this.content >= this.buffer.length - 2 * sLen) {
			resize(2 * sLen);
		}
		for (int i = 0; i < sLen; i++) {
			writeChar(s.charAt(i));
		}
	}

	@Override
	public void writeDouble(final double v) throws IOException {
		writeLong(Double.doubleToLongBits(v));
	}

	@Override
	public void writeFloat(final float v) throws IOException {
		writeInt(Float.floatToIntBits(v));
	}

	@SuppressWarnings("restriction")
	@Override
	public void writeInt(int v) throws IOException {
		if (this.content >= this.buffer.length - 3) {
			resize(4);
		}
		if (LITTLE_ENDIAN) {
			v = Integer.reverseBytes(v);
		}
		UNSAFE.putInt(this.buffer, BASE_OFFSET + this.content, v);
		this.content += 4;
	}

	@SuppressWarnings("restriction")
	@Override
	public void writeLong(long v) throws IOException {
		if (this.content >= this.buffer.length - 7) {
			resize(8);
		}
		if (LITTLE_ENDIAN) {
			v = Long.reverseBytes(v);
		}
		UNSAFE.putLong(this.buffer, BASE_OFFSET + this.content, v);
		this.content += 8;
	}

	@Override
	public void writeShort(final int v) throws IOException {
		if (this.content >= this.buffer.length - 1) {
			resize(2);
		}
		this.buffer[this.content++] = (byte) ((v >>> 8) & 0xff);
		this.buffer[this.content++] = (byte) (v & 0xff);
	}

	@Override
	public void writeUTF(final String str) throws IOException {
		final int strlen = str.length();
		int utflen = 0;
		int c;

		/* use charAt instead of copying String to char array */
		for (int i = 0; i < strlen; i++) {
			c = str.charAt(i);
			if ((c >= 0x0001) && (c <= 0x007F)) {
				utflen++;
			} else if (c > 0x07FF) {
				utflen += 3;
			} else {
				utflen += 2;
			}
		}

		if (utflen > 65535) {
			throw new UTFDataFormatException("Encoded string is too long: " + utflen);
		}
		else if (this.content > this.buffer.length - utflen - 2) {
			resize(utflen + 2);
		}

		final byte[] bytearr = this.buffer;
		int count = this.content;

		bytearr[count++] = (byte) ((utflen >>> 8) & 0xFF);
		bytearr[count++] = (byte) ((utflen) & 0xFF);

		int i;
		for (i = 0; i < strlen; i++) {
			c = str.charAt(i);
			if (!((c >= 0x0001) && (c <= 0x007F))) {
				break;
			}
			bytearr[count++] = (byte) c;
		}

		for (; i < strlen; i++) {
			c = str.charAt(i);
			if ((c >= 0x0001) && (c <= 0x007F)) {
				bytearr[count++] = (byte) c;

			} else if (c > 0x07FF) {
				bytearr[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
				bytearr[count++] = (byte) (0x80 | ((c >> 6) & 0x3F));
				bytearr[count++] = (byte) (0x80 | (c & 0x3F));
			} else {
				bytearr[count++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
				bytearr[count++] = (byte) (0x80 | (c & 0x3F));
			}
		}

		this.content = count;
	}

	@Override
	public void skipBytesToWrite(final int numBytes) throws IOException {
		if (buffer.length - this.content < numBytes) {
			throw new EOFException("Could not skip " + numBytes + " bytes.");
		}

		this.content += numBytes;
	}

	@Override
	public void write(final DataInputView source, final int numBytes) throws IOException {
		if (buffer.length - this.content < numBytes) {
			throw new EOFException("Could not write " + numBytes + " bytes. Buffer overflow.");
		}

		source.readFully(this.buffer, this.content, numBytes);
		this.content += numBytes;
	}

	// ----------------------------------------------------------------------------------------
	//                               Data Input
	// ----------------------------------------------------------------------------------------

	@Override
	public boolean readBoolean() throws IOException {
		if (this.position < content) {
			return this.buffer[this.position++] != 0;
		} else {
			throw new EOFException();
		}
	}

	@Override
	public byte readByte() throws IOException {
		if (this.position < content) {
			return this.buffer[this.position++];
		} else {
			throw new EOFException();
		}
	}

	@Override
	public char readChar() throws IOException {
		if (this.position < content - 1) {
			return (char) (((this.buffer[this.position++] & 0xff) << 8) | (this.buffer[this.position++] & 0xff));
		} else {
			throw new EOFException();
		}
	}

	@Override
	public double readDouble() throws IOException {
		return Double.longBitsToDouble(readLong());
	}

	@Override
	public float readFloat() throws IOException {
		return Float.intBitsToFloat(readInt());
	}

	@Override
	public void readFully(final byte[] b) throws IOException {
		readFully(b, 0, b.length);
	}

	@Override
	public void readFully(final byte[] b, final int off, final int len) throws IOException {
		if (len >= 0) {
			if (off <= b.length - len) {
				if (this.position <= content - len) {
					System.arraycopy(this.buffer, position, b, off, len);
					position += len;
				} else {
					throw new EOFException();
				}
			} else {
				throw new ArrayIndexOutOfBoundsException();
			}
		} else if (len < 0) {
			throw new IllegalArgumentException("Length may not be negative.");
		}
	}

	@Override
	public int readInt() throws IOException {
		if (this.position >= 0 && this.position < content - 3) {
			@SuppressWarnings("restriction")
			int value = UNSAFE.getInt(this.buffer, BASE_OFFSET + this.position);
			if (LITTLE_ENDIAN) {
				value = Integer.reverseBytes(value);
			}

			this.position += 4;
			return value;
		} else {
			throw new EOFException();
		}
	}

	@Override
	public String readLine() throws IOException {
		if (this.position < content) {
			// read until a newline is found
			final StringBuilder bld = new StringBuilder();
			char curr = (char) readUnsignedByte();
			while (position < content && curr != '\n') {
				bld.append(curr);
				curr = (char) readUnsignedByte();
			}
			// trim a trailing carriage return
			final int len = bld.length();
			if (len > 0 && bld.charAt(len - 1) == '\r') {
				bld.setLength(len - 1);
			}
			String s = bld.toString();
			bld.setLength(0);
			return s;
		} else {
			return null;
		}
	}

	@Override
	public long readLong() throws IOException {
		if (position >= 0 && position < content - 7) {
			@SuppressWarnings("restriction")
			long value = UNSAFE.getLong(this.buffer, BASE_OFFSET + this.position);
			if (LITTLE_ENDIAN) {
				value = Long.reverseBytes(value);
			}
			this.position += 8;
			return value;
		} else {
			throw new EOFException();
		}
	}

	@Override
	public short readShort() throws IOException {
		if (position >= 0 && position < content - 1) {
			return (short) ((((this.buffer[position++]) & 0xff) << 8) | ((this.buffer[position++]) & 0xff));
		} else {
			throw new EOFException();
		}
	}

	@Override
	public String readUTF() throws IOException {
		final int utflen = readUnsignedShort();
		final byte[] bytearr = new byte[utflen];
		final char[] chararr = new char[utflen];

		int c, char2, char3;
		int count = 0;
		int chararrCount = 0;

		readFully(bytearr, 0, utflen);

		while (count < utflen) {
			c = (int) bytearr[count] & 0xff;
			if (c > 127) {
				break;
			}
			count++;
			chararr[chararrCount++] = (char) c;
		}

		while (count < utflen) {
			c = (int) bytearr[count] & 0xff;
			switch (c >> 4) {
			case 0:
			case 1:
			case 2:
			case 3:
			case 4:
			case 5:
			case 6:
			case 7:
				/* 0xxxxxxx */
				count++;
				chararr[chararrCount++] = (char) c;
				break;
			case 12:
			case 13:
				/* 110x xxxx 10xx xxxx */
				count += 2;
				if (count > utflen) {
					throw new UTFDataFormatException("malformed input: partial character at end");
				}
				char2 = (int) bytearr[count - 1];
				if ((char2 & 0xC0) != 0x80) {
					throw new UTFDataFormatException("malformed input around byte " + count);
				}
				chararr[chararrCount++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
				break;
			case 14:
				/* 1110 xxxx 10xx xxxx 10xx xxxx */
				count += 3;
				if (count > utflen) {
					throw new UTFDataFormatException("malformed input: partial character at end");
				}
				char2 = (int) bytearr[count - 2];
				char3 = (int) bytearr[count - 1];
				if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80)) {
					throw new UTFDataFormatException("malformed input around byte " + (count - 1));
				}
				chararr[chararrCount++] = (char) (((c & 0x0F) << 12) | ((char2 & 0x3F) << 6) | (char3 & 0x3F));
				break;
			default:
				/* 10xx xxxx, 1111 xxxx */
				throw new UTFDataFormatException("malformed input around byte " + count);
			}
		}
		// The number of chars produced may be less than utflen
		return new String(chararr, 0, chararrCount);
	}

	@Override
	public int readUnsignedByte() throws IOException {
		if (this.position < content) {
			return (this.buffer[this.position++] & 0xff);
		} else {
			throw new EOFException();
		}
	}

	@Override
	public int readUnsignedShort() throws IOException {
		if (this.position < content - 1) {
			return ((this.buffer[this.position++] & 0xff) << 8) | (this.buffer[this.position++] & 0xff);
		} else {
			throw new EOFException();
		}
	}

	@Override
	public int skipBytes(int n) throws IOException {
		if (this.position <= content - n) {
			this.position += n;
			return n;
		} else {
			n = content - this.position;
			this.position = content;
			return n;
		}
	}

	@Override
	public void skipBytesToRead(final int numBytes) throws IOException {
		int skippedBytes = skipBytes(numBytes);

		if (skippedBytes < numBytes) {
			throw new EOFException("Could not skip " + numBytes + " bytes.");
		}
	}

	@Override
	public int read(final byte[] b, final int off, final int len) throws IOException {
		if (b == null) {
			throw new NullPointerException("Byte array b cannot be null.");
		}

		if (off < 0) {
			throw new IndexOutOfBoundsException("Offset cannot be negative.");
		}

		if (len < 0) {
			throw new IndexOutOfBoundsException("Length cannot be negative.");
		}

		if (b.length - off < len) {
			throw new IndexOutOfBoundsException("Byte array does not provide enough space to store requested data" +
					".");
		}

		if (this.position >= content) {
			return -1;
		} else {
			int toRead = Math.min(content - this.position, len);
			System.arraycopy(this.buffer, this.position, b, off, toRead);
			this.position += toRead;

			return toRead;
		}
	}

	@Override
	public int read(final byte[] b) throws IOException {
		return read(b, 0, b.length);
	}

	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("restriction")
	private static final sun.misc.Unsafe UNSAFE = MemoryUtils.UNSAFE;

	@SuppressWarnings("restriction")
	private static final long BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

	private static final boolean LITTLE_ENDIAN = (MemoryUtils.NATIVE_BYTE_ORDER == ByteOrder.LITTLE_ENDIAN);
}
