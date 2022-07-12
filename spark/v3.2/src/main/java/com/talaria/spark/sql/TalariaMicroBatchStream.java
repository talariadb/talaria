package com.talaria.spark.sql;

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TalariaMicroBatchStream implements MicroBatchStream {
    private static final Logger LOG = LoggerFactory.getLogger(TalariaMicroBatchStream.class);
    private final String tableName;
    private final StructType schema;
    private final String partitionBy;
    private final String checkpointLocation;
    Long latestOffsetValue = System.currentTimeMillis()/1000;
    Long initialOffsetValue = latestOffsetValue - 10;

    TalariaMicroBatchStream(String tableName, StructType schema, String partitionBy, String checkpointLocation) {
        this.tableName = tableName;
        this.schema = schema;
        this.partitionBy = partitionBy;
        this.checkpointLocation = checkpointLocation;
    }

    @Override
    public Offset latestOffset() {
        initialOffsetValue = latestOffsetValue;
        latestOffsetValue = System.currentTimeMillis()/1000;
        return new TalariaOffset(latestOffsetValue);
    }

    @Override
    public InputPartition[] planInputPartitions(Offset start, Offset end) {
        Long startOffset = ((TalariaOffset) start).getOffset();
        Long endOffset = ((TalariaOffset) end).getOffset();
        return new TalariaMicroBatchStreamPartition[]{new TalariaMicroBatchStreamPartition(startOffset, endOffset, "127.0.0.1", 8043)};
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new TalariaMicroBatchStreamPartitionReaderFactory(tableName, schema, partitionBy);
    }

    @Override
    public Offset initialOffset() {
        return new TalariaOffset(initialOffsetValue);
    }

    @Override
    public Offset deserializeOffset(String json) {
        return new TalariaOffset(initialOffsetValue);
    }

    @Override
    public void commit(Offset end) {

    }

    @Override
    public void stop() {

    }
}