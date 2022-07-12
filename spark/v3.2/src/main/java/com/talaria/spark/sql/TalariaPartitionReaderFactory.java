package com.talaria.spark.sql;

import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class TalariaPartitionReaderFactory implements PartitionReaderFactory {

    private final String tableName;
    private final StructType schema;

    TalariaPartitionReaderFactory(String tableName, StructType schema){
        this.tableName = tableName;
        this.schema = schema;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        return new TalariaPartitionReader("127.0.0.1", 8043,  tableName, schema);
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
        TalariaPartition p = (TalariaPartition) partition;
        return new TalariaColumnarPartitionReader("127.0.0.1", p.port, p.partitionBy, tableName, schema);
    }

    @Override
    public boolean supportColumnarReads(InputPartition partition) {
        return true;
    }

}