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

package com.ververica.cdc.connectors.tidb.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import com.ververica.cdc.connectors.tidb.TiKVSnapshotEventDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.key.RowKey;
import org.tikv.kvproto.Kvrpcpb.KvPair;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.tikv.common.codec.TableCodec.decodeObjects;

/**
 * Deserialization schema from TiKV Snapshot Event to Flink Table/SQL internal data structure {@link
 * RowData}.
 */
public class RowDataTiKVSnapshotEventDeserializationSchema
        extends RowDataTiKVEventDeserializationSchemaBase
        implements TiKVSnapshotEventDeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(RowDataTiKVSnapshotEventDeserializationSchema.class);

    /** TypeInformation of the produced {@link RowData}. * */
    private final TypeInformation<RowData> resultTypeInfo;

    public RowDataTiKVSnapshotEventDeserializationSchema(
            TiConfiguration tiConf,
            String database,
            String tableName,
            TypeInformation<RowData> resultTypeInfo,
            TiKVMetadataConverter[] metadataConverters,
            RowType physicalDataType) {
        super(tiConf, database, tableName, metadataConverters, physicalDataType);
        this.resultTypeInfo = checkNotNull(resultTypeInfo);
    }

    @Override
    public TypeInformation getProducedType() {
        return resultTypeInfo;
    }

    @Override
    public void deserialize(KvPair record, Collector<RowData> out) throws Exception {
        if (tableInfo == null) {
            tableInfo = fetchTableInfo();
        }
        RowKey.Handle handle =
                RowKey.decodeHandle(record.getKey().toByteArray(), tableInfo.isCommonHandle());

        Object[] tikvValues;
        if (handle.getIsCommonHandle()) {
            tikvValues =
                    decodeObjects(
                            record.getValue().toByteArray(), handle.getStringHandle(), tableInfo);
        } else {
            tikvValues =
                    decodeObjects(
                            record.getValue().toByteArray(), handle.getLongHandle(), tableInfo);
        }
        emit(
                new TiKVMetadataConverter.TiKVRowValue(record),
                (RowData) physicalConverter.convert(tikvValues, tableInfo, null),
                out);
    }
}
