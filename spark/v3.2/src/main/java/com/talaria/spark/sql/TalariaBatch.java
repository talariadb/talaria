package com.talaria.spark.sql;

import com.google.protobuf.ByteString;
import com.talaria.client.TalariaClient;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.List;

public class TalariaBatch implements Batch {

    private final String tableName;
    private final StructType schema;
    private final String partitionBy;

    TalariaBatch(String tableName, StructType schema, String partitionBy) {
        this.tableName = tableName;
        this.schema = schema;
        this.partitionBy = partitionBy;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        //int[] talariaNodes = {8059, 8060, 8061, 8062};
        //List<TalariaPartition> partitions = new java.util.ArrayList<>(Collections.emptyList());
        //for (int talariaNode : talariaNodes) {
        //    partitions.add(new TalariaPartition("127.0.0.1", talariaNode, this.partitionBy));

        //}
        //TalariaPartition[] talariaPartitions = new TalariaPartition[partitions.size()];
        //return partitions.toArray(talariaPartitions);
        return new TalariaPartition[]{new TalariaPartition("127.0.0.1", 8043, partitionBy)};
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new TalariaPartitionReaderFactory(tableName, schema);
    }
}