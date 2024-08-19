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
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.ververica.cdc.connectors.tidb.table.RowDataTiKVChangeEventDeserializationSchema;
import com.ververica.cdc.connectors.tidb.table.RowDataTiKVSnapshotEventDeserializationSchema;
import com.ververica.cdc.connectors.tidb.table.StartupMode;
import com.ververica.cdc.connectors.tidb.table.utils.TableKeyRangeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.Kvrpcpb;

import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static org.tikv.kvproto.Cdcpb.Event.Row.OpType.DELETE;

/**
 * The source implementation for TiKV that read snapshot events first and then read the change
 * event.
 */
public class TiKVRichParallelSourceFunctionSingleton<T> extends RichParallelSourceFunction<T>
        implements CheckpointListener, CheckpointedFunction, ResultTypeQueryable<T> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG =
            LoggerFactory.getLogger(TiKVRichParallelSourceFunctionSingleton.class);

    private final TiKVSnapshotEventDeserializationSchema<T> snapshotEventDeserializationSchema;
    private final TiKVChangeEventDeserializationSchema<T> changeEventDeserializationSchema;
    private final TiConfiguration tiConf;
    private final StartupMode startupMode;

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

    private final TiTableInfo tableInfo;
    protected transient TargetPartitionFilter partitionFilter;

    public TiKVRichParallelSourceFunctionSingleton(
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

        tableInfo = TiKVSource.getTiKVSource(tiConf).getTable(database, tableName);

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
    }

    @Override
    public void open(final Configuration config) throws Exception {
        super.open(config);

        prewrites = new TreeMap<>();
        commits = new TreeMap<>();
        // cdc event will lose if pull cdc event block when region split
        // use queue to separate read and write to ensure pull event unblock.
        // since sink jdbc is slow, 5000W queue size may be safe size.
        committedEvents = new LinkedBlockingQueue<>();
        outputCollector = new OutputCollector<>();
        resolvedTs =
                startupMode == StartupMode.INITIAL
                        ? TiKVSource.SNAPSHOT_VERSION_EPOCH
                        : TiKVSource.STREAMING_VERSION_START_EPOCH;
        ThreadFactory threadFactory =
                new ThreadFactoryBuilder()
                        .setNameFormat(
                                tableInfo.getName()
                                        + "-tidb-source-function-"
                                        + getRuntimeContext().getIndexOfThisSubtask())
                        .build();
        executorService = Executors.newSingleThreadExecutor(threadFactory);

        // add for hash key to subtask, then drop record of other subtask
        int maxParallel = getRuntimeContext().getMaxNumberOfParallelSubtasks();
        int parallel = getRuntimeContext().getNumberOfParallelSubtasks();
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        partitionFilter = new TargetPartitionFilter(maxParallel, parallel, subtaskIndex, tableInfo);
    }

    @Override
    public void run(final SourceContext<T> ctx) throws Exception {
        sourceContext = ctx;
        outputCollector.context = sourceContext;

        TiKVSource tiKVSource = TiKVSource.getTiKVSource(tiConf);
        if (startupMode == StartupMode.INITIAL) {
            synchronized (sourceContext.getCheckpointLock()) {
                LOG.info("read snapshot events");
                resolvedTs = tiKVSource.scanTable(tableInfo, this::sinkSnapshotEvents);
            }
        } else {
            LOG.info("Skip snapshot read");
            resolvedTs = tiKVSource.getResolvedTs();
        }
        LOG.info("start read change events");
        running = true;
        sinkChangeEventsAsync();

        tiKVSource.captureChangeData(
                tableInfo, resolvedTs, this::handleRow, this::advanceResolvedTs);
    }

    private void handleRow(final Cdcpb.Event.Row row) {
        byte[] rowKey = row.getKey().toByteArray();
        boolean isCommonHandle = tableInfo.isCommonHandle();
        RowKey.Handle handle = RowKey.decodeHandle(rowKey, isCommonHandle);

        byte[] rowValue;
        if (row.getOpType() == DELETE) {
            rowValue = row.getOldValue().toByteArray();
        } else {
            rowValue = row.getValue().toByteArray();
        }

        if (!TableKeyRangeUtils.isRecordKey(rowKey) || !partitionFilter.filter(handle, rowValue)) {
            // Don't handle index key for now
            return;
        }
        LOG.debug("binlog record, type: {}, data: {}", row.getType(), row);
        switch (row.getType()) {
            case COMMITTED:
                prewrites.put(RowKeyWithTs.ofStart(row, handle), row);
                commits.put(RowKeyWithTs.ofCommit(row, handle), row);
                break;
            case COMMIT:
                commits.put(RowKeyWithTs.ofCommit(row, handle), row);
                break;
            case PREWRITE:
                prewrites.put(RowKeyWithTs.ofStart(row, handle), row);
                break;
            case ROLLBACK:
                prewrites.remove(RowKeyWithTs.ofStart(row, handle));
                break;
            default:
                LOG.warn("Unsupported row type:" + row.getType());
        }
    }

    protected void sinkSnapshotEvents(Kvrpcpb.KvPair pair) {
        byte[] rowKey = pair.getKey().toByteArray();
        RowKey.Handle handle = RowKey.decodeHandle(rowKey, tableInfo.isCommonHandle());
        if (TableKeyRangeUtils.isRecordKey(rowKey)
                && partitionFilter.filter(handle, pair.getValue().toByteArray())) {
            try {
                snapshotEventDeserializationSchema.deserialize(pair, outputCollector);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected void sinkChangeEventsAsync() {
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
    }

    protected void advanceResolvedTs(Long resolvedTs) {
        this.resolvedTs = resolvedTs;
        if (!commits.isEmpty()) {
            try {
                flushRows(resolvedTs);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected void flushRows(final long timestamp) throws Exception {
        Preconditions.checkState(sourceContext != null, "sourceContext shouldn't be null");
        synchronized (sourceContext) {
            while (!commits.isEmpty() && commits.firstKey().timestamp <= timestamp) {
                final Cdcpb.Event.Row commitRow = commits.pollFirstEntry().getValue();
                final Cdcpb.Event.Row prewriteRow =
                        prewrites.remove(
                                RowKeyWithTs.ofStart(
                                        commitRow,
                                        RowKey.decodeHandle(
                                                commitRow.getKey().toByteArray(),
                                                tableInfo.isCommonHandle())));
                // if pull cdc event block when region split, cdc event will lose.
                committedEvents.offer(prewriteRow);
            }
        }
    }

    @Override
    public void cancel() {
        try {
            running = false;
            TiKVSource.getTiKVSource(tiConf).cancelCaptureChangeData(tableInfo);
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
        private final RowKey.Handle handle;

        private RowKeyWithTs(final long timestamp, final RowKey.Handle handle) {
            this.timestamp = timestamp;
            this.handle = handle;
        }

        private RowKeyWithTs(final long timestamp, final byte[] key) {
            this(timestamp, RowKey.decodeHandle(key, false));
        }

        @Override
        public int compareTo(final RowKeyWithTs that) {
            int res = Long.compare(this.timestamp, that.timestamp);
            if (res == 0) {
                res = this.handle.compareTo(that.handle);
            }
            return res;
        }

        @Override
        public int hashCode() {
            if (this.handle.getIsCommonHandle()) {
                return Objects.hash(
                        this.timestamp, this.handle.getTableId(), this.handle.getStringHandle());
            }
            return Objects.hash(
                    this.timestamp, this.handle.getTableId(), this.handle.getLongHandle());
        }

        @Override
        public boolean equals(final Object thatObj) {
            if (thatObj instanceof RowKeyWithTs) {
                final RowKeyWithTs that = (RowKeyWithTs) thatObj;
                return this.timestamp == that.timestamp && this.handle.equals(that.handle);
            }
            return false;
        }

        static RowKeyWithTs ofStart(final Cdcpb.Event.Row row, RowKey.Handle handle) {
            return new RowKeyWithTs(row.getStartTs(), handle);
        }

        static RowKeyWithTs ofCommit(final Cdcpb.Event.Row row, RowKey.Handle handle) {
            return new RowKeyWithTs(row.getCommitTs(), handle);
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
