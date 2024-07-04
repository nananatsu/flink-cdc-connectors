/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.tidb.table.utils;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.binary.BinaryRowData;

import java.util.Arrays;

/** copied from org.apache.flink.table.data.writer.AbstractBinaryWriter. */
public class BinaryWriterUtils {

    public static int getFixedLengthPartSize(int nullBitsSizeInBytes, int arity) {
        return nullBitsSizeInBytes + 8 * arity;
    }

    public static int getFieldOffset(int nullBitsSizeInBytes, int pos) {
        return nullBitsSizeInBytes + 8 * pos;
    }

    public static void writeBytesToFixLenPart(
            MemorySegment segment, int fieldOffset, byte[] bytes, int len) {
        long firstByte = len | 0x80; // first bit is 1, other bits is len
        long sevenBytes = 0L; // real data
        if (BinaryRowData.LITTLE_ENDIAN) {
            for (int i = 0; i < len; i++) {
                sevenBytes |= ((0x00000000000000FFL & bytes[i]) << (i * 8L));
            }
        } else {
            for (int i = 0; i < len; i++) {
                sevenBytes |= ((0x00000000000000FFL & bytes[i]) << ((6 - i) * 8L));
            }
        }

        final long offsetAndSize = (firstByte << 56) | sevenBytes;

        segment.putLong(fieldOffset, offsetAndSize);
    }

    public static int roundNumberOfBytesToNearestWord(int numBytes) {
        int remainder = numBytes & 0x07;
        if (remainder == 0) {
            return numBytes;
        } else {
            return numBytes + (8 - remainder);
        }
    }

    public static MemorySegment grow(MemorySegment segment, int minCapacity) {
        int oldCapacity = segment.size();
        int newCapacity = oldCapacity + (oldCapacity >> 1);
        if (newCapacity - minCapacity < 0) {
            newCapacity = minCapacity;
        }
        segment = MemorySegmentFactory.wrap(Arrays.copyOf(segment.getArray(), newCapacity));
        //        afterGrow();
        return segment;
    }
}
