package com.talaria.spark.sql;

import org.apache.spark.sql.connector.read.InputPartition;

public class TalariaPartition implements InputPartition{
    public final String host;
    public final int port;
    public final Long start;
    public final Long end;
    TalariaPartition(String host, int port, Long start, Long end) {
       this.host = host;
       this.port = port;
       this.start = start;
       this.end = end;
    }
}