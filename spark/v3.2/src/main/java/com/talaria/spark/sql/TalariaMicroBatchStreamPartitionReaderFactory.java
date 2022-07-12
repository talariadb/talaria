package com.talaria.spark.sql;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class TalariaMicroBatchStreamPartitionReaderFactory implements PartitionReaderFactory {

    private final String tableName;
    private final StructType schema;
    private final String partitionBy;

    TalariaMicroBatchStreamPartitionReaderFactory(String tableName, StructType schema, String partitionBy){
        this.tableName = tableName;
        this.schema = schema;
        this.partitionBy = partitionBy;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        return null;
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
        TalariaMicroBatchStreamPartition p = (TalariaMicroBatchStreamPartition) partition;
        return new TalariaMicroBatchStreamPartitionReader(p.host, p.port, tableName, schema, partitionBy, p.start, p.end);
    }

    @Override
    public boolean supportColumnarReads(InputPartition partition) {
        return true;
    }
}
