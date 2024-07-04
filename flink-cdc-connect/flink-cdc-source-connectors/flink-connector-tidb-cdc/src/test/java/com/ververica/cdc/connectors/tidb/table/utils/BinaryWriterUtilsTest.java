package com.ververica.cdc.connectors.tidb.table.utils;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

/** check row key hash same as BinaryRowData. */
class BinaryWriterUtilsTest {

    @Test
    void testRowStringToSegment() {
        String str = "test_20240702_130911";
        GenericRowData rowData = new GenericRowData(RowKind.INSERT, 1);
        rowData.setField(0, StringData.fromString(str));

        BinaryRowData row = new BinaryRowData(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);

        writer.reset();
        writer.writeString(0, rowData.getString(0));
        writer.complete();

        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        int len = bytes.length;
        MemorySegment memorySegment = MemorySegmentFactory.wrap(new byte[40]);

        int nullBitsSizeInBytes = BinaryRowData.calculateBitSetWidthInBytes(1);
        int fieldOffset = BinaryWriterUtils.getFieldOffset(nullBitsSizeInBytes, 0);

        //  writer.reset();
        int cursor = BinaryWriterUtils.getFixedLengthPartSize(nullBitsSizeInBytes, 1);
        for (int i = 0; i < nullBitsSizeInBytes; i += 8) {
            memorySegment.putLong(i, 0L);
        }

        // BinaryFormat.MAX_FIX_PART_DATA_SIZE 7
        int length = len;
        if (len <= 7) {
            BinaryWriterUtils.writeBytesToFixLenPart(memorySegment, fieldOffset, bytes, len);
        } else {
            final int roundedSize = BinaryWriterUtils.roundNumberOfBytesToNearestWord(len);

            // ensureCapacity(roundedSize);
            length = cursor + roundedSize;
            if (memorySegment.size() < length) {
                memorySegment = BinaryWriterUtils.grow(memorySegment, length);
            }

            // zeroOutPaddingBytes(len);
            if ((len & 0x07) > 0) {
                memorySegment.putLong(cursor + ((len >> 3) << 3), 0L);
            }

            memorySegment.put(cursor, bytes, 0, len);

            // setOffsetAndSize(pos, cursor, len);
            final long offsetAndSize = ((long) cursor << 32) | len;
            memorySegment.putLong(fieldOffset, offsetAndSize);
        }

        Assertions.assertEquals(
                row.hashCode(), MurmurHashUtils.hashBytesByWords(memorySegment, 0, length));

        BinaryRowData expectRow = new BinaryRowData(1);
        expectRow.pointTo(memorySegment, 0, length);

        Assertions.assertEquals(str, expectRow.getString(0).toString());
    }
}
