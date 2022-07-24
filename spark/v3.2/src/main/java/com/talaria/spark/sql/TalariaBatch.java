package com.talaria.spark.sql;

import com.google.protobuf.ByteString;
import com.talaria.client.TalariaClient;
import com.talaria.protos.Endpoint;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TalariaBatch implements Batch {

    private final String tableName;
    private final StructType schema;
    private final String partitionFilter;
    private final String talariaDomain;
    private final int talariaPort;
    private final String hashBy;
    private final String sortBy;
    private final String talariaSchema;
    private final Long from;
    private final Long until;

    // Keep from and until as options for sortBy

    TalariaBatch(TalariaTable table, ReadOptions options) {
        this.tableName = table.name();
        this.schema = table.schema();
        this.talariaDomain = options.getDomain();
        this.talariaPort = options.getPort();
        this.talariaSchema = options.getSchema();
        this.hashBy = table.getHashBy();
        this.sortBy = table.getSortBy();
        this.partitionFilter = options.getPartitionFilter();
        this.from = options.getFromTimestamp();
        this.until = options.getUntilTimestamp();
    }

    @Override
    public InputPartition[] planInputPartitions() {

        TalariaClient tc = new TalariaClient(this.talariaDomain, this.talariaPort);
        List<Endpoint> nodes = tc.getNodes();
        tc.close();
        List<TalariaPartition> partitions = new ArrayList<>(Collections.emptyList());
        for (Endpoint node: nodes) {
            partitions.add(new TalariaPartition(node.getHost(), node.getPort(), from, until));
        }
        TalariaPartition[] talariaPartitions = new TalariaPartition[partitions.size()];
        return partitions.toArray(talariaPartitions);
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new TalariaPartitionReaderFactory(tableName, schema, talariaSchema, hashBy, sortBy, partitionFilter);
    }
}