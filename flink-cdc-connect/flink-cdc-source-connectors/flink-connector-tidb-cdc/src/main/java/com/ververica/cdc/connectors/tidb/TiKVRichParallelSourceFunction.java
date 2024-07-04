/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.tidb;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.ververica.cdc.connectors.tidb.table.RowDataTiKVChangeEventDeserializationSchema;
import com.ververica.cdc.connectors.tidb.table.RowDataTiKVSnapshotEventDeserializationSchema;
import com.ververica.cdc.connectors.tidb.table.StartupMode;
import com.ververica.cdc.connectors.tidb.table.utils.BinaryWriterUtils;
import com.ververica.cdc.connectors.tidb.table.utils.MurmurHashUtils;
import com.ververica.cdc.connectors.tidb.table.utils.TableKeyRangeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.cdc.CDCClient;
import org.tikv.common.PDClient;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.codec.CodecDataInput;
import org.tikv.common.codec.RowDecoderV2;
import org.tikv.common.codec.RowV2;
import org.tikv.common.exception.CodecException;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.operation.PDErrorHandler;
import org.tikv.common.types.IntegerType;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.Coprocessor;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.PDGrpc;
import org.tikv.kvproto.Pdpb;
import org.tikv.shade.com.google.protobuf.ByteString;
import org.tikv.txn.KVClient;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.tikv.common.pd.PDError.buildFromPdpbError;

/**
 * The source implementation for TiKV that read snapshot events first and then read the change
 * event.
 */
public class TiKVRichParallelSourceFunction<T> extends RichParallelSourceFunction<T>
        implements CheckpointListener, CheckpointedFunction, ResultTypeQueryable<T> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(TiKVRichParallelSourceFunction.class);
    private static final long SNAPSHOT_VERSION_EPOCH = -1L;
    private static final long STREAMING_VERSION_START_EPOCH = 0L;

    private final TiKVSnapshotEventDeserializationSchema<T> snapshotEventDeserializationSchema;
    private final TiKVChangeEventDeserializationSchema<T> changeEventDeserializationSchema;
    private final TiConfiguration tiConf;
    private final StartupMode startupMode;
    private final String database;
    private final String tableName;

    /** Task local variables. */
    private transient TiSession session = null;

    private transient Coprocessor.KeyRange keyRange = null;
    private transient CDCClient cdcClient = null;
    private transient SourceContext<T> sourceContext = null;
    private transient volatile long resolvedTs = -1L;
    private transient TreeMap<RowKeyWithTs, Cdcpb.Event.Row> prewrites = null;
    private transient TreeMap<RowKeyWithTs, Cdcpb.Event.Row> commits = null;
    private transient BlockingQueue<Cdcpb.Event.Row> committedEvents = null;
    private transient OutputCollector<T> outputCollector;

    private transient boolean running = true;
    private transient ExecutorService executorService;

    /** offset state. */
    private transient ListState<Long> offsetState;

    private static final long CLOSE_TIMEOUT = 30L;

    TiTableInfo tableInfo;
    //    private final Long tableId;
    protected transient BiFunction<Long, byte[], Boolean> checkPartition;
    protected transient MemorySegment memorySegment;

    public TiKVRichParallelSourceFunction(
            TiKVSnapshotEventDeserializationSchema<T> snapshotEventDeserializationSchema,
            TiKVChangeEventDeserializationSchema<T> changeEventDeserializationSchema,
            TiConfiguration tiConf,
            StartupMode startupMode,
            String database,
            String tableName) {
        this.snapshotEventDeserializationSchema = snapshotEventDeserializationSchema;
        this.changeEventDeserializationSchema = changeEventDeserializationSchema;
        this.tiConf = tiConf;
        this.startupMode = startupMode;
        this.database = database;
        this.tableName = tableName;

        try (final TiSession session = TiSession.create(tiConf)) {
            tableInfo = session.getCatalog().getTable(database, tableName);
            if (tableInfo == null) {
                throw new RuntimeException(
                        String.format("Table %s.%s does not exist.", database, tableName));
            }
            if (snapshotEventDeserializationSchema
                    instanceof RowDataTiKVSnapshotEventDeserializationSchema) {
                ((RowDataTiKVSnapshotEventDeserializationSchema) snapshotEventDeserializationSchema)
                        .updateTableInfo(tableInfo);
            }
            if (changeEventDeserializationSchema
                    instanceof RowDataTiKVChangeEventDeserializationSchema) {
                ((RowDataTiKVChangeEventDeserializationSchema) changeEventDeserializationSchema)
                        .updateTableInfo(tableInfo);
            }
            //            tableId = tableInfo.getId();
        } catch (final Exception e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    public void open(final Configuration config) throws Exception {
        super.open(config);
        session = TiSession.create(tiConf);
        //        TiTableInfo tableInfo = session.getCatalog().getTable(database, tableName);
        //        if (tableInfo == null) {
        //            throw new RuntimeException(
        //                    String.format("Table %s.%s does not exist.", database, tableName));
        //        }
        //        long tableId = tableInfo.getId();

        keyRange = TableKeyRangeUtils.getTableKeyRange(tableInfo.getId(), 1, 0);
        //        keyRange =
        //                TableKeyRangeUtils.getTableKeyRange(
        //                        tableId,
        //                        getRuntimeContext().getNumberOfParallelSubtasks(),
        //                        getRuntimeContext().getIndexOfThisSubtask());
        cdcClient = new CDCClient(session, keyRange);
        prewrites = new TreeMap<>();
        commits = new TreeMap<>();
        // cdc event will lose if pull cdc event block when region split
        // use queue to separate read and write to ensure pull event unblock.
        // since sink jdbc is slow, 5000W queue size may be safe size.
        committedEvents = new LinkedBlockingQueue<>();
        outputCollector = new OutputCollector<>();
        resolvedTs =
                startupMode == StartupMode.INITIAL
                        ? SNAPSHOT_VERSION_EPOCH
                        : STREAMING_VERSION_START_EPOCH;
        ThreadFactory threadFactory =
                new ThreadFactoryBuilder()
                        .setNameFormat(
                                "tidb-source-function-"
                                        + getRuntimeContext().getIndexOfThisSubtask())
                        .build();
        executorService = Executors.newSingleThreadExecutor(threadFactory);

        // add for hash key to subtask, then drop record of other subtask
        int maxParallel = getRuntimeContext().getMaxNumberOfParallelSubtasks();
        int parallel = getRuntimeContext().getNumberOfParallelSubtasks();
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

        BiFunction<Long, byte[], Boolean> checkPartition;
        if (!tableInfo.isPkHandle() && tableInfo.hasPrimaryKey()) {
            memorySegment = MemorySegmentFactory.allocateUnpooledSegment(40);
            try {
                Field field = tableInfo.getClass().getDeclaredField("primaryKeyColumn");
                field.setAccessible(true);
                TiColumnInfo primaryColumn = ((TiColumnInfo) field.get(tableInfo));
                //                final int offset = primaryColumn.getOffset();
                final String colName = primaryColumn.getName();

                checkPartition =
                        (Long handle, byte[] values) -> {
                            Object pk = getRowKey(values, handle, tableInfo, primaryColumn);
                            if (pk == null) {
                                throw new RuntimeException(
                                        "primary column "
                                                + colName
                                                + " not exist in table "
                                                + tableName);
                            }
                            if (pk instanceof String) {
                                byte[] bytes = ((String) pk).getBytes(StandardCharsets.UTF_8);
                                int len = bytes.length;

                                int nullBitsSizeInBytes =
                                        BinaryRowData.calculateBitSetWidthInBytes(1);
                                int fieldOffset =
                                        BinaryWriterUtils.getFieldOffset(nullBitsSizeInBytes, 0);

                                //  writer.reset();
                                int cursor =
                                        BinaryWriterUtils.getFixedLengthPartSize(
                                                nullBitsSizeInBytes, 1);
                                for (int i = 0; i < nullBitsSizeInBytes; i += 8) {
                                    memorySegment.putLong(i, 0L);
                                }
                                // BinaryFormat.MAX_FIX_PART_DATA_SIZE 7
                                if (len <= 7) {
                                    BinaryWriterUtils.writeBytesToFixLenPart(
                                            memorySegment, fieldOffset, bytes, len);
                                } else {
                                    final int roundedSize =
                                            BinaryWriterUtils.roundNumberOfBytesToNearestWord(len);

                                    // ensureCapacity(roundedSize);
                                    final int length = cursor + roundedSize;
                                    if (memorySegment.size() < length) {
                                        memorySegment =
                                                BinaryWriterUtils.grow(memorySegment, length);
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

                                int hash = MurmurHashUtils.hashBytesByWords(memorySegment, 0, len);
                                int keyGroup = MathUtils.murmurHash(hash) % maxParallel;
                                int targetSubpartition = keyGroup * parallel / maxParallel;

                                LOG.debug(
                                        " PK: {} ,segment: {}  ,HashCode: {} ,KeyGroup: {} ,SubPartition: {} , SubtaskIndex: {} ,Drop: {}",
                                        pk,
                                        memorySegment.getArray(),
                                        hash,
                                        keyGroup,
                                        targetSubpartition,
                                        subtaskIndex,
                                        targetSubpartition != subtaskIndex);
                                return targetSubpartition == subtaskIndex;

                            } else {
                                throw new RuntimeException(
                                        "primary column " + colName + " type not support");
                            }
                        };
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        } else {
            memorySegment = MemorySegmentFactory.allocateUnpooledSegment(Long.BYTES * 2);
            checkPartition =
                    (Long handle, byte[] values) -> {
                        memorySegment.putLong(8, handle);
                        int hash =
                                MurmurHashUtils.hashBytesByWords(memorySegment, 0, Long.BYTES * 2);
                        int keyGroup = MathUtils.murmurHash(hash) % maxParallel;
                        int targetSubpartition = keyGroup * parallel / maxParallel;
                        return targetSubpartition == subtaskIndex;
                    };
        }
        this.checkPartition = checkPartition;
    }

    @Override
    public void run(final SourceContext<T> ctx) throws Exception {
        sourceContext = ctx;
        outputCollector.context = sourceContext;

        if (startupMode == StartupMode.INITIAL) {
            synchronized (sourceContext.getCheckpointLock()) {
                readSnapshotEvents();
            }
        } else {
            LOG.info("Skip snapshot read");
            resolvedTs = session.getTimestamp().getVersion();
            //            resolvedTs = getGcSafePoint();
        }

        LOG.info("start read change events");
        cdcClient.start(resolvedTs);
        running = true;
        readChangeEvents();
    }

    public long getGcSafePoint() throws NoSuchFieldException, IllegalAccessException {
        PDClient client = session.getPDClient();
        Field headerField = PDClient.class.getDeclaredField("header");
        headerField.setAccessible(true);
        Pdpb.RequestHeader header = (Pdpb.RequestHeader) headerField.get(client);

        Supplier<Pdpb.GetGCSafePointRequest> request =
                () -> Pdpb.GetGCSafePointRequest.newBuilder().setHeader(header).build();
        PDErrorHandler<Pdpb.GetGCSafePointResponse> handler =
                new PDErrorHandler<>(
                        r ->
                                r.getHeader().hasError()
                                        ? buildFromPdpbError(r.getHeader().getError())
                                        : null,
                        session.getPDClient());

        Pdpb.GetGCSafePointResponse resp =
                client.callWithRetry(
                        ConcreteBackOffer.newTsoBackOff(),
                        PDGrpc.getGetGCSafePointMethod(),
                        request,
                        handler);

        return resp.getSafePoint();
    }

    private Object getRowKey(
            byte[] value, Long handle, TiTableInfo tableInfo, TiColumnInfo primaryColumn) {
        if (value.length == 0) {
            throw new CodecException("Decode fails: value length is zero");
        }
        if (handle == null && tableInfo.isPkHandle()) {
            throw new IllegalArgumentException("when pk is handle, handle cannot be null");
        }
        Object pk = null;
        if ((value[0] & 0xff) == RowV2.CODEC_VER) {
            // decode bytes
            RowV2 rowV2 = RowV2.createNew(value);
            RowV2.ColIDSearchResult searchResult = rowV2.findColID(primaryColumn.getId());

            // corresponding column should be found
            byte[] colData = rowV2.getData(searchResult.getIdx());
            pk = RowDecoderV2.decodeCol(colData, primaryColumn.getType());
            LOG.info("##### PK: {} ,Raw: {}", pk, colData);
        } else {
            // decode bytes
            CodecDataInput cdi = new CodecDataInput(value);
            if (primaryColumn.getId() == 1L) {
                if (!cdi.eof()) {
                    pk = primaryColumn.getType().decodeForBatchWrite(cdi);
                }
            } else {
                int colSize = tableInfo.getColumns().size();
                HashMap<Long, TiColumnInfo> idToColumn = new HashMap<>(colSize);
                for (TiColumnInfo col : tableInfo.getColumns()) {
                    idToColumn.put(col.getId(), col);
                }
                while (!cdi.eof()) {
                    long colID = (long) IntegerType.BIGINT.decode(cdi);
                    Object colValue = idToColumn.get(colID).getType().decodeForBatchWrite(cdi);
                    if (colID == primaryColumn.getId()) {
                        pk = colValue;
                        break;
                    }
                }
            }
        }
        return pk;
    }

    private void handleRow(final Cdcpb.Event.Row row) {
        byte[] rowKey = row.getKey().toByteArray();
        if (!TableKeyRangeUtils.isRecordKey(rowKey)
                || !checkPartition.apply(
                        RowKey.decode(rowKey).getHandle(), row.getValue().toByteArray())) {
            //            if (!TableKeyRangeUtils.isRecordKey(row.getKey().toByteArray())) {
            // Don't handle index key for now
            return;
        }
        LOG.debug("binlog record, type: {}, data: {}", row.getType(), row);
        switch (row.getType()) {
            case COMMITTED:
                prewrites.put(RowKeyWithTs.ofStart(row), row);
                commits.put(RowKeyWithTs.ofCommit(row), row);
                break;
            case COMMIT:
                commits.put(RowKeyWithTs.ofCommit(row), row);
                break;
            case PREWRITE:
                prewrites.put(RowKeyWithTs.ofStart(row), row);
                break;
            case ROLLBACK:
                prewrites.remove(RowKeyWithTs.ofStart(row));
                break;
            default:
                LOG.warn("Unsupported row type:" + row.getType());
        }
    }

    protected void readSnapshotEvents() throws Exception {
        LOG.info("read snapshot events");
        try (KVClient scanClient = session.createKVClient()) {
            //            long startTs = session.getTimestamp().getVersion();
            long startTs = getGcSafePoint();
            ByteString start = keyRange.getStart();
            while (true) {
                final List<Kvrpcpb.KvPair> segment =
                        scanClient.scan(start, keyRange.getEnd(), startTs);

                if (segment.isEmpty()) {
                    resolvedTs = startTs;
                    break;
                }

                for (final Kvrpcpb.KvPair pair : segment) {
                    byte[] rowKey = pair.getKey().toByteArray();
                    if (!TableKeyRangeUtils.isRecordKey(rowKey)
                            || !checkPartition.apply(
                                    RowKey.decode(rowKey).getHandle(),
                                    pair.getValue().toByteArray())) {
                        //                        if
                        // (TableKeyRangeUtils.isRecordKey(pair.getKey().toByteArray())) {
                        snapshotEventDeserializationSchema.deserialize(pair, outputCollector);
                    }
                }

                start =
                        RowKey.toRawKey(segment.get(segment.size() - 1).getKey())
                                .next()
                                .toByteString();
            }
        }
    }

    protected void readChangeEvents() throws Exception {
        LOG.info("read change event from resolvedTs:{}", resolvedTs);
        // child thread to sink committed rows.
        executorService.execute(
                () -> {
                    while (running) {
                        try {
                            Cdcpb.Event.Row committedRow = committedEvents.take();
                            changeEventDeserializationSchema.deserialize(
                                    committedRow, outputCollector);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
        while (resolvedTs >= STREAMING_VERSION_START_EPOCH) {
            for (int i = 0; i < 1000; i++) {
                final Cdcpb.Event.Row row = cdcClient.get();
                if (row == null) {
                    break;
                }
                handleRow(row);
            }
            resolvedTs = cdcClient.getMaxResolvedTs();
            if (commits.size() > 0) {
                flushRows(resolvedTs);
            }
        }
    }

    protected void flushRows(final long timestamp) throws Exception {
        Preconditions.checkState(sourceContext != null, "sourceContext shouldn't be null");
        synchronized (sourceContext) {
            while (!commits.isEmpty() && commits.firstKey().timestamp <= timestamp) {
                final Cdcpb.Event.Row commitRow = commits.pollFirstEntry().getValue();
                final Cdcpb.Event.Row prewriteRow =
                        prewrites.remove(RowKeyWithTs.ofStart(commitRow));
                // if pull cdc event block when region split, cdc event will lose.
                committedEvents.offer(prewriteRow);
            }
        }
    }

    @Override
    public void cancel() {
        try {
            running = false;
            if (cdcClient != null) {
                cdcClient.close();
            }
            if (executorService != null) {
                executorService.shutdown();
                if (!executorService.awaitTermination(CLOSE_TIMEOUT, TimeUnit.SECONDS)) {
                    LOG.warn(
                            "Failed to close the tidb source function in {} seconds.",
                            CLOSE_TIMEOUT);
                }
            }
        } catch (final Exception e) {
            LOG.error("Unable to close cdcClient", e);
        }
    }

    @Override
    public void snapshotState(final FunctionSnapshotContext context) throws Exception {
        LOG.info(
                "snapshotState checkpoint: {} at resolvedTs: {}",
                context.getCheckpointId(),
                resolvedTs);
        flushRows(resolvedTs);
        offsetState.clear();
        offsetState.add(resolvedTs);
    }

    @Override
    public void initializeState(final FunctionInitializationContext context) throws Exception {
        LOG.info("initialize checkpoint");
        offsetState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "resolvedTsState", LongSerializer.INSTANCE));
        if (context.isRestored()) {
            for (final Long offset : offsetState.get()) {
                resolvedTs = offset;
                LOG.info("Restore State from resolvedTs: {}", resolvedTs);
                return;
            }
        } else {
            resolvedTs = 0;
            LOG.info("Initialize State from resolvedTs: {}", resolvedTs);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // do nothing
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return snapshotEventDeserializationSchema.getProducedType();
    }

    // ---------------------------------------
    // static Utils classes
    // ---------------------------------------
    private static class RowKeyWithTs implements Comparable<RowKeyWithTs> {
        private final long timestamp;
        private final RowKey rowKey;

        private RowKeyWithTs(final long timestamp, final RowKey rowKey) {
            this.timestamp = timestamp;
            this.rowKey = rowKey;
        }

        private RowKeyWithTs(final long timestamp, final byte[] key) {
            this(timestamp, RowKey.decode(key));
        }

        @Override
        public int compareTo(final RowKeyWithTs that) {
            int res = Long.compare(this.timestamp, that.timestamp);
            if (res == 0) {
                res = Long.compare(this.rowKey.getTableId(), that.rowKey.getTableId());
            }
            if (res == 0) {
                res = Long.compare(this.rowKey.getHandle(), that.rowKey.getHandle());
            }
            return res;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.timestamp, this.rowKey.getTableId(), this.rowKey.getHandle());
        }

        @Override
        public boolean equals(final Object thatObj) {
            if (thatObj instanceof RowKeyWithTs) {
                final RowKeyWithTs that = (RowKeyWithTs) thatObj;
                return this.timestamp == that.timestamp && this.rowKey.equals(that.rowKey);
            }
            return false;
        }

        static RowKeyWithTs ofStart(final Cdcpb.Event.Row row) {
            return new RowKeyWithTs(row.getStartTs(), row.getKey().toByteArray());
        }

        static RowKeyWithTs ofCommit(final Cdcpb.Event.Row row) {
            return new RowKeyWithTs(row.getCommitTs(), row.getKey().toByteArray());
        }
    }

    private static class OutputCollector<T> implements Collector<T> {

        private SourceContext<T> context;

        @Override
        public void collect(T record) {
            context.collect(record);
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
