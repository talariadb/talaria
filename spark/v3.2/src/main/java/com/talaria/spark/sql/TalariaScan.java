package com.talaria.spark.sql;

import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.streaming.ContinuousStream;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.*;

public class TalariaScan implements Scan {

    private final TalariaTable table;
    private final ReadOptions options;

    TalariaScan(TalariaTable table){
        this.options = table.getReadOptions();
        this.table = table;
    }
    @Override
    public StructType readSchema() {
        return this.table.schema();
    }

    @Override
    public Batch toBatch() {
        return new TalariaBatch(table.name(), table.schema(), options.getPartitionFilter());
    }

    @Override
    public MicroBatchStream toMicroBatchStream(String checkpointLocation){
        return new TalariaMicroBatchStream(table.name(), table.schema(), options.getPartitionFilter(), checkpointLocation);
    }

    @Override
    public ContinuousStream toContinuousStream(String checkpointLocation) {
        return new TalariaContinuousStream(table.name(), table.schema(), options.getPartitionFilter(), checkpointLocation);
    }
}