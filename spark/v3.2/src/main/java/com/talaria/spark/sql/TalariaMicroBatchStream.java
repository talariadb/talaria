package com.talaria.spark.sql;

import com.google.common.base.Preconditions;
import com.talaria.client.TalariaClient;
import com.talaria.protos.Endpoint;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TalariaMicroBatchStream implements MicroBatchStream {
    private static final Logger LOG = LoggerFactory.getLogger(TalariaMicroBatchStream.class);
    private final String tableName;
    private final StructType schema;
    private final String partitionFilter;
    private final String talariaDomain;
    private final int talariaPort;
    private final String hashBy;
    private final String sortBy;
    private final String talariaSchema;
    private final TalariaOffsetStore offsetStore;
    private final TalariaOffset initialOffset;
    Long latestOffsetValue;
    Long initialOffsetValue;

    TalariaMicroBatchStream(JavaSparkContext context, TalariaTable table, ReadOptions options,  String checkpointLocation) throws IOException {
        this.tableName = table.name();
        this.schema = table.schema();
        this.talariaDomain = options.getDomain();
        this.talariaPort = options.getPort();
        this.partitionFilter = options.getPartitionFilter();
        this.talariaSchema = options.getSchema();
        this.hashBy = table.getHashBy();
        this.sortBy = table.getSortBy();
        latestOffsetValue = System.currentTimeMillis()/1000;
        initialOffsetValue = latestOffsetValue - 10;
        this.offsetStore = new TalariaOffsetStore(context.hadoopConfiguration(), checkpointLocation, initialOffsetValue);
        this.initialOffset = this.offsetStore.initialOffset();
    }

    @Override
    public Offset latestOffset() {
        initialOffsetValue = latestOffsetValue;
        latestOffsetValue = System.currentTimeMillis()/1000;
        return new TalariaOffset(latestOffsetValue);
    }

    @Override
    public InputPartition[] planInputPartitions(Offset start, Offset end) {
        Preconditions.checkArgument(start instanceof TalariaOffset, "Invalid start offset: %s is not a TalariaOffset", start);
        Preconditions.checkArgument(end instanceof TalariaOffset, "Invalid end offset: %s is not a TalariaOffset", end);
        Long startOffset = ((TalariaOffset) start).getOffset();
        Long endOffset = ((TalariaOffset) end).getOffset();
        TalariaClient tc = new TalariaClient(this.talariaDomain, this.talariaPort);
        List<Endpoint> nodes = tc.getNodes();
        tc.close();
        List<TalariaPartition> partitions = new ArrayList<>(Collections.emptyList());
        for (Endpoint node: nodes) {
            partitions.add(new TalariaPartition(node.getHost(), node.getPort(), startOffset, endOffset));
        }
        TalariaPartition[] talariaPartitions = new TalariaPartition[partitions.size()];
        return partitions.toArray(talariaPartitions);
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new TalariaPartitionReaderFactory(tableName, schema, talariaSchema, hashBy, sortBy, partitionFilter);
    }

    @Override
    public Offset initialOffset() {
        return this.initialOffset;
    }

    @Override
    public Offset deserializeOffset(String json) {
        return TalariaOffset.fromJSON(json);
    }

    @Override
    public void commit(Offset end) {
        TalariaOffset endOffset = (TalariaOffset) end;
        try {
            this.offsetStore.updateOffset(endOffset);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {

    }
}