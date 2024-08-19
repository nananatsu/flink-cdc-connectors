package com.ververica.cdc.connectors.tidb;

import com.ververica.cdc.connectors.tidb.table.utils.TableKeyRangeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.cdc.CDCClient;
import org.tikv.common.PDClient;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.operation.PDErrorHandler;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.Coprocessor;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.PDGrpc;
import org.tikv.kvproto.Pdpb;
import org.tikv.shade.com.google.protobuf.ByteString;
import org.tikv.txn.KVClient;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.tikv.common.pd.PDError.buildFromPdpbError;

/** capture tikv data use same session. */
public class TiKVSource {

    private static final Logger LOG = LoggerFactory.getLogger(TiKVSource.class);

    private static ConcurrentHashMap<Integer, TiKVSource> tiKVSources = new ConcurrentHashMap<>();

    public static final long STREAMING_VERSION_START_EPOCH = 0L;
    public static final long SNAPSHOT_VERSION_EPOCH = -1L;

    private final TiConfiguration tiConf;
    private transient TiSession session;

    private final ConcurrentHashMap<String, TiTableInfo> tableInfoCache;
    private transient ConcurrentHashMap<TiTableInfo, CDCClient> cdcClients;

    private TiKVSource(final TiConfiguration tiConf) {
        this.tiConf = tiConf;
        session = TiSession.create(tiConf);
        tableInfoCache = new ConcurrentHashMap<>();
        cdcClients = new ConcurrentHashMap<>();
    }

    public TiTableInfo getTable(String database, String tableName) {
        String fullName = database + "." + tableName;
        TiTableInfo tableInfo = tableInfoCache.get(fullName);
        if (tableInfo != null) {
            return tableInfo;
        }

        tableInfo = session.getCatalog().getTable(database, tableName);
        if (tableInfo == null) {
            throw new RuntimeException(
                    String.format("Table %s.%s does not exist.", database, tableName));
        }
        tableInfoCache.put(fullName, tableInfo);
        return tableInfo;
    }

    public long getResolvedTs() {
        return session.getTimestamp().getVersion();
    }

    public long getGcSafePoint() throws NoSuchFieldException, IllegalAccessException {
        //        try (PDClient client = session.getPDClient()) {
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
        //        } catch (InterruptedException e) {
        //            throw new RuntimeException(e);
        //        }
    }

    public void captureChangeData(
            TiTableInfo tableInfo,
            long resolvedTs,
            Consumer<Cdcpb.Event.Row> consumer,
            Consumer<Long> advance)
            throws InterruptedException {
        LOG.info("read change event from resolvedTs:{}", resolvedTs);

        Coprocessor.KeyRange keyRange =
                TableKeyRangeUtils.getTableKeyRange(tableInfo.getId(), 1, 0);
        CDCClient client = new CDCClient(session, keyRange);
        cdcClients.put(tableInfo, client);
        client.start(resolvedTs);

        while (resolvedTs >= TiKVSource.STREAMING_VERSION_START_EPOCH) {
            for (int i = 0; i < 1000; i++) {
                final Cdcpb.Event.Row row = client.get();
                if (row == null) {
                    break;
                }
                consumer.accept(row);
            }
            resolvedTs = client.getMaxResolvedTs();
            advance.accept(resolvedTs);
        }
    }

    public void cancelCaptureChangeData(TiTableInfo tableInfo) {
        CDCClient client = cdcClients.remove(tableInfo);
        if (client != null) {
            client.close();
        }
    }

    public long scanTable(TiTableInfo tableInfo, Consumer<Kvrpcpb.KvPair> consumer) {
        Coprocessor.KeyRange keyRange =
                TableKeyRangeUtils.getTableKeyRange(tableInfo.getId(), 1, 0);

        try (KVClient scanClient = session.createKVClient()) {
            long startTs = getGcSafePoint();
            ByteString start = keyRange.getStart();

            while (true) {
                final List<Kvrpcpb.KvPair> segment =
                        scanClient.scan(start, keyRange.getEnd(), startTs);

                if (segment.isEmpty()) {
                    return startTs;
                }

                for (final Kvrpcpb.KvPair pair : segment) {
                    consumer.accept(pair);
                }

                start =
                        RowKey.toRawKey(segment.get(segment.size() - 1).getKey())
                                .next()
                                .toByteString();
            }
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static int hashcode(TiConfiguration tiConf) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(out)) {
            oos.writeObject(tiConf);
            return Arrays.hashCode(out.toByteArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static TiKVSource getTiKVSource(final TiConfiguration tiConf) {
        int h = hashcode(tiConf);
        TiKVSource tiKVSource = tiKVSources.get(h);
        if (tiKVSource == null) {
            synchronized (TiKVSource.class) {
                tiKVSource = tiKVSources.get(h);
                if (tiKVSource == null) {
                    tiKVSource = new TiKVSource(tiConf);
                    tiKVSources.put(h, tiKVSource);
                }
            }
        }
        return tiKVSource;
    }
}
