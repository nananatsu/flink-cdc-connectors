package com.ververica.cdc.connectors.tidb.table.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** check row key hash same as BinaryRowData. */
class BinaryWriterUtilsTest {

    @Test
    void testRowStringToSegment() {
        String[] strs = new String[] {"111222", "test_20240702_130911"};
        for (String str : strs) {
            GenericRowData rowData = new GenericRowData(RowKind.INSERT, 1);
            rowData.setField(0, StringData.fromString(str));

            BinaryRowData row = new BinaryRowData(1);
            BinaryRowWriter writer = new BinaryRowWriter(row);

            writer.reset();
            writer.writeString(0, rowData.getString(0));
            writer.complete();

            MemorySegment memorySegment = MemorySegmentFactory.wrap(new byte[40]);
            Tuple2<MemorySegment, Integer> segment =
                    BinaryWriterUtils.writeMemorySegment(memorySegment, str);

            Assertions.assertEquals(
                    row.hashCode(), MurmurHashUtils.hashBytesByWords(segment.f0, 0, segment.f1));

            BinaryRowData expectRow = new BinaryRowData(1);
            expectRow.pointTo(memorySegment, 0, segment.f1);

            Assertions.assertEquals(str, expectRow.getString(0).toString());
        }
    }
}
