/*
 * Copyright 2021 TiKV Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.tikv.common.key;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.binary.BinaryRowData;

import com.ververica.cdc.connectors.tidb.table.utils.BinaryWriterUtils;
import com.ververica.cdc.connectors.tidb.table.utils.MurmurHashUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.codec.Codec.BytesCodec;
import org.tikv.common.codec.Codec.IntegerCodec;
import org.tikv.common.codec.CodecDataInput;
import org.tikv.common.codec.CodecDataOutput;
import org.tikv.common.codec.RowDecoderV2;
import org.tikv.common.codec.RowV2;
import org.tikv.common.exception.CodecException;
import org.tikv.common.exception.TiClientInternalException;
import org.tikv.common.exception.TiExpressionException;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.types.IntegerType;
import org.tikv.shade.com.google.common.primitives.UnsignedBytes;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import static org.tikv.common.codec.Codec.BYTES_FLAG;
import static org.tikv.common.codec.Codec.IntegerCodec.writeLong;

/** Copied from https://github.com/tikv/client-java project to fix. */
public class RowKey extends Key implements Serializable {
    private static final byte[] REC_PREFIX_SEP = new byte[] {'_', 'r'};

    private static final Logger LOG = LoggerFactory.getLogger(RowKey.class);

    private final long tableId;
    private final long handle;
    private final boolean maxHandleFlag;

    private RowKey(long tableId, long handle) {
        super(encode(tableId, handle));
        this.tableId = tableId;
        this.handle = handle;
        this.maxHandleFlag = false;
    }

    /**
     * The RowKey indicating maximum handle (its value exceeds Long.Max_Value)
     *
     * <p>Initializes an imaginary globally MAXIMUM rowKey with tableId.
     */
    private RowKey(long tableId) {
        super(encodeBeyondMaxHandle(tableId));
        this.tableId = tableId;
        this.handle = Long.MAX_VALUE;
        this.maxHandleFlag = true;
    }

    public static RowKey toRowKey(long tableId, long handle) {
        return new RowKey(tableId, handle);
    }

    public static RowKey toRowKey(long tableId, org.tikv.common.key.TypedKey handle) {
        Object obj = handle.getValue();
        if (obj instanceof Long) {
            return new RowKey(tableId, (long) obj);
        }
        throw new TiExpressionException("Cannot encode row key with non-long type");
    }

    public static Object getRowPK(byte[] value, TiTableInfo tableInfo, TiColumnInfo primaryColumn) {
        if (value.length == 0) {
            throw new CodecException("Decode fails: value length is zero");
        }
        Object pk = null;
        if ((value[0] & 0xff) == RowV2.CODEC_VER) {
            // decode bytes
            RowV2 rowV2 = RowV2.createNew(value);
            RowV2.ColIDSearchResult searchResult = rowV2.findColID(primaryColumn.getId());
            if (searchResult.getIdx() == -1) {
                return null;
            }
            // corresponding column should be found
            byte[] colData = rowV2.getData(searchResult.getIdx());
            pk = RowDecoderV2.decodeCol(colData, primaryColumn.getType());
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

    public static RowKey createMin(long tableId) {
        return toRowKey(tableId, Long.MIN_VALUE);
    }

    public static RowKey createBeyondMax(long tableId) {
        return new RowKey(tableId);
    }

    public static RowKey decode(byte[] value) {
        CodecDataInput cdi = new CodecDataInput(value);
        cdi.readByte();
        long tableId = IntegerCodec.readLong(cdi); // tableId

        cdi.readByte();
        cdi.readByte();

        long handle = IntegerCodec.readLong(cdi); // handle
        return toRowKey(tableId, handle);
    }

    public static Handle decodeHandle(byte[] value, boolean isCommonHandle) {
        return new Handle(value, isCommonHandle);
    }

    private static byte[] encode(long tableId, long handle) {
        CodecDataOutput cdo = new CodecDataOutput();
        encodePrefix(cdo, tableId);
        writeLong(cdo, handle);
        return cdo.toBytes();
    }

    private static byte[] encodeBeyondMaxHandle(long tableId) {
        return prefixNext(encode(tableId, Long.MAX_VALUE));
    }

    private static void encodePrefix(CodecDataOutput cdo, long tableId) {
        cdo.write(TBL_PREFIX);
        writeLong(cdo, tableId);
        cdo.write(REC_PREFIX_SEP);
    }

    @Override
    public RowKey next() {
        long handle = getHandle();
        boolean maxHandleFlag = getMaxHandleFlag();
        if (maxHandleFlag) {
            throw new TiClientInternalException("Handle overflow for Long MAX");
        }
        if (handle == Long.MAX_VALUE) {
            return createBeyondMax(tableId);
        }
        return new RowKey(tableId, handle + 1);
    }

    public long getTableId() {
        return tableId;
    }

    public long getHandle() {
        return handle;
    }

    private boolean getMaxHandleFlag() {
        return maxHandleFlag;
    }

    @Override
    public String toString() {
        return Long.toString(handle);
    }

    /** Copied from https://github.com/tikv/client-java project to fix. */
    public static class DecodeResult {
        public long handle;
        public Status status;

        /** DecodeResult Status. */
        public enum Status {
            MIN,
            MAX,
            EQUAL,
            LESS,
            GREATER,
            UNKNOWN_INF
        }
    }

    /** tidb clustered index allow handle is pk(not only number pk). */
    public static class Handle implements Serializable, Comparable<Handle> {
        private final boolean isCommonHandle;
        private final long tableId;
        private final byte[] handle;
        private transient MemorySegment memorySegment;

        private Handle(long tableId, byte[] handle, boolean isCommonHandle) {
            this.isCommonHandle = isCommonHandle;
            this.tableId = tableId;
            this.handle = handle;
        }

        public Handle(byte[] rowKey, boolean isCommonHandle) {
            this.isCommonHandle = isCommonHandle;
            CodecDataInput cdi = new CodecDataInput(rowKey);
            cdi.readByte();
            this.tableId = IntegerCodec.readLong(cdi); // tableId

            cdi.readByte();
            cdi.readByte();

            if (isCommonHandle) {
                if (cdi.readByte() != BYTES_FLAG) {
                    throw new RuntimeException("not support handle type");
                }
            }
            try {
                if (rowKey.length == 19) {
                    this.handle = new byte[8];
                    cdi.readFully(this.handle);
                } else {
                    this.handle = BytesCodec.readBytes(cdi);
                }
            } catch (Exception e) {
                Handle ret = RowKey.decodeHandle(rowKey, isCommonHandle);
                LOG.error("read handle {} fail ", ret, e);
                throw new RuntimeException("read handle fail" + e.getMessage());
            }
        }

        public boolean getIsCommonHandle() {
            return isCommonHandle;
        }

        public long getTableId() {
            return tableId;
        }

        public String getStringHandle() {
            return new String(handle, StandardCharsets.UTF_8);
        }

        public byte[] getBytesHandle() {
            return handle;
        }

        public Long getLongHandle() {
            return IntegerCodec.readLong(new CodecDataInput(handle));
        }

        public MemorySegment getMemorySegment() {
            return this.memorySegment;
        }

        public void setMemorySegment(MemorySegment memorySegment) {
            this.memorySegment = memorySegment;
        }

        @Override
        public int compareTo(RowKey.Handle that) {
            int res = Long.compare(this.getTableId(), that.getTableId());
            if (res == 0) {
                if (isCommonHandle) {
                    res =
                            UnsignedBytes.lexicographicalComparator()
                                    .compare(this.getBytesHandle(), that.getBytesHandle());
                } else {
                    res = Long.compare(this.getLongHandle(), that.getLongHandle());
                }
            }
            return res;
        }

        @Override
        public int hashCode() {
            int hash;
            if (isCommonHandle) {
                if (this.memorySegment == null) {
                    memorySegment = MemorySegmentFactory.allocateUnpooledSegment(64);
                }
                Tuple2<MemorySegment, Integer> segment =
                        BinaryWriterUtils.writeMemorySegment(memorySegment, getStringHandle());
                if (segment.f0.size() > memorySegment.size()) {
                    memorySegment = segment.f0;
                }
                hash = MurmurHashUtils.hashBytesByWords(segment.f0, 0, segment.f1);
            } else {
                if (this.memorySegment == null) {
                    memorySegment = MemorySegmentFactory.allocateUnpooledSegment(Long.BYTES * 2);
                }
                int fieldOffset =
                        BinaryWriterUtils.getFieldOffset(
                                BinaryRowData.calculateBitSetWidthInBytes(1), 0);

                memorySegment.putLong(fieldOffset, getLongHandle());
                hash = MurmurHashUtils.hashBytesByWords(memorySegment, 0, fieldOffset + 8);
            }
            return hash;
        }

        @Override
        public boolean equals(Object other) {
            if (other == this) {
                return true;
            }
            if (other instanceof Handle) {
                return compareTo((Handle) other) == 0;
            } else {
                return false;
            }
        }
    }
}
