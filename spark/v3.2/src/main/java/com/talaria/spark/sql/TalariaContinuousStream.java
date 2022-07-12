package com.talaria.spark.sql;

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.streaming.ContinuousPartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.ContinuousStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.PartitionOffset;
import org.apache.spark.sql.types.StructType;

public class TalariaContinuousStream implements ContinuousStream {

    private final String tableName;
    private final StructType schema;
    private final String partitionBy;
    private final String checkpointLocation;
    TalariaContinuousStream(String tableName, StructType schema, String partitionBy, String checkpointLocation) {
        this.tableName = tableName;
        this.schema = schema;
        this.partitionBy = partitionBy;
        this.checkpointLocation = checkpointLocation;
    }
    @Override
    public InputPartition[] planInputPartitions(Offset start) {
        TalariaOffset startOffset = (TalariaOffset) start;
        return new InputPartition[]{new TalariaContinuousStreamPartition("127.0.0.1",8043, 0, startOffset.getOffset())};
    }

    @Override
    public ContinuousPartitionReaderFactory createContinuousReaderFactory() {
        return new TalariaContinuousPartitionReaderFactory(tableName, schema, partitionBy);
    }

    @Override
    public Offset mergeOffsets(PartitionOffset[] offsets) {
        if (offsets.length == 0) {
            return new TalariaOffset(System.currentTimeMillis()/1000);
        }
        TalariaContinuousStreamPartitionOffset tcp = (TalariaContinuousStreamPartitionOffset) offsets[0];
        return new TalariaOffset(tcp.offset);
    }

    @Override
    public Offset initialOffset() {
        return new TalariaOffset(System.currentTimeMillis()/1000);
    }

    @Override
    public Offset deserializeOffset(String json) {
        return new TalariaOffset(System.currentTimeMillis()/1000);
    }

    @Override
    public void commit(Offset end) {

    }

    @Override
    public void stop() {

    }
}