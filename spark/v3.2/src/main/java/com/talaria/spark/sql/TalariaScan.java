package com.talaria.spark.sql;

import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.streaming.ContinuousStream;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.*;

import java.io.IOException;
import java.util.Objects;

public class TalariaScan implements Scan {

    private final JavaSparkContext sparkContext;
    private final TalariaTable table;
    private final ReadOptions options;

    TalariaScan(SparkSession spark, TalariaTable table){
        this.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
        this.options = table.getReadOptions();
        this.table = table;
    }
    @Override
    public StructType readSchema() {
        return this.table.schema();
    }

    @Override
    public Batch toBatch() {
        return new TalariaBatch(table, options);
    }

    @Override
    public MicroBatchStream toMicroBatchStream(String checkpointLocation){
        // use the user defined checkpoint location if its set.
        checkpointLocation = !Objects.equals(options.getCheckpointLocation(), "") ? options.getCheckpointLocation(): checkpointLocation;
        try {
            return new TalariaMicroBatchStream(sparkContext, table, options, checkpointLocation);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ContinuousStream toContinuousStream(String checkpointLocation) {
        return new TalariaContinuousStream(table.name(), table.schema(), options.getPartitionFilter(), checkpointLocation);
    }
}