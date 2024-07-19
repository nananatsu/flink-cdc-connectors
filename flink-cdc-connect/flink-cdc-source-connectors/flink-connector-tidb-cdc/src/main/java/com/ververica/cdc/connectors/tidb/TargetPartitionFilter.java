package com.ververica.cdc.connectors.tidb;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.util.MathUtils;

import com.ververica.cdc.connectors.tidb.table.utils.BinaryWriterUtils;
import com.ververica.cdc.connectors.tidb.table.utils.MurmurHashUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiTableInfo;

import java.lang.reflect.Field;
import java.util.function.BiFunction;

/** capture full table range, then use hash-filter(same as flink partition selector) filter row. */
public class TargetPartitionFilter {

    private static final Logger LOG = LoggerFactory.getLogger(TargetPartitionFilter.class);

    private int maxParallel;
    private int parallel;
    private int subtaskIndex;
    private TiTableInfo tableInfo;
    private TiColumnInfo primaryColumn;
    private MemorySegment memorySegment;
    private BiFunction<RowKey.Handle, byte[], Boolean> filter;

    public TargetPartitionFilter(
            int maxParallel, int parallel, int subtaskIndex, TiTableInfo tableInfo)
            throws NoSuchFieldException, IllegalAccessException {
        this.maxParallel = maxParallel;
        this.parallel = parallel;
        this.subtaskIndex = subtaskIndex;
        this.tableInfo = tableInfo;

        if (!tableInfo.isPkHandle() && tableInfo.hasPrimaryKey()) {
            Field field = tableInfo.getClass().getDeclaredField("primaryKeyColumn");
            field.setAccessible(true);
            primaryColumn = ((TiColumnInfo) field.get(tableInfo));
            memorySegment = MemorySegmentFactory.allocateUnpooledSegment(64);
            filter = this::commonPKFilter;
        } else {
            primaryColumn = null;
            memorySegment = MemorySegmentFactory.allocateUnpooledSegment(Long.BYTES * 2);
            filter = this::longPKFilter;
        }
    }

    public boolean filter(RowKey.Handle key, byte[] values) {
        return this.filter.apply(key, values);
    }

    public boolean longPKFilter(RowKey.Handle key, byte[] values) {
        key.setMemorySegment(memorySegment);
        int hash = key.hashCode();
        int keyGroup = MathUtils.murmurHash(hash) % maxParallel;
        int targetSubpartition = keyGroup * parallel / maxParallel;
        return targetSubpartition == subtaskIndex;
    }

    public boolean commonPKFilter(RowKey.Handle key, byte[] values) {
        Object pk;
        int hash;
        // tidb 8.0 clustered index pk not exist in row
        // key contains pk
        if (key.getIsCommonHandle()) {
            pk = key.getStringHandle();
            key.setMemorySegment(memorySegment);
            hash = key.hashCode();
            if (key.getMemorySegment().size() > memorySegment.size()) {
                memorySegment = key.getMemorySegment();
            }
        } else {
            pk = RowKey.getRowPK(values, tableInfo, primaryColumn);
            if (pk == null) {
                throw new RuntimeException(
                        "primary column "
                                + primaryColumn.getName()
                                + " not exist in table"
                                + tableInfo.getName());
            }

            if (pk instanceof String) {
                Tuple2<MemorySegment, Integer> segment =
                        BinaryWriterUtils.writeMemorySegment(memorySegment, (String) pk);
                if (segment.f0.size() > memorySegment.size()) {
                    memorySegment = segment.f0;
                }
                hash = MurmurHashUtils.hashBytesByWords(segment.f0, 0, segment.f1);
            } else {
                throw new RuntimeException(
                        "primary column " + primaryColumn.getName() + " type not support");
            }
        }

        int keyGroup = MathUtils.murmurHash(hash) % maxParallel;
        int targetSubpartition = keyGroup * parallel / maxParallel;

        LOG.debug(
                "table: {} ,PK: {} ,HashCode: {} ,KeyGroup: {} ,SubPartition: {} ,SubtaskIndex: {} ,Drop: {}",
                tableInfo.getName(),
                pk,
                hash,
                keyGroup,
                targetSubpartition,
                subtaskIndex,
                targetSubpartition != subtaskIndex);
        return targetSubpartition == subtaskIndex;
    }
}
