package com.talaria.spark.sql;

import org.apache.spark.sql.connector.read.InputPartition;

public class TalariaContinuousStreamPartition implements InputPartition {
    String host;
    int port;
    int index;
    long offset;
    TalariaContinuousStreamPartition(String host, int port, int index, long start) {
        this.host = host;
        this.port = port;
        this.index = index;
        this.offset = start;
    }
}
