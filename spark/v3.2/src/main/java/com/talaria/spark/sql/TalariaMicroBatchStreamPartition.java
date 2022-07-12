package com.talaria.spark.sql;

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.types.StructType;

public class TalariaMicroBatchStreamPartition implements InputPartition {
    public final String host;
    public final int port;
    public final Long start;
    public final Long end;

    TalariaMicroBatchStreamPartition(Long start, Long end, String host, int port) {
        this.start = start;
        this.end = end;
        this.host = host;
        this.port = port;
    }
}