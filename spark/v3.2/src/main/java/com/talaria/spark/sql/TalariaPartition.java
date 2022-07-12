package com.talaria.spark.sql;

import com.google.protobuf.ByteString;
import org.apache.spark.sql.connector.read.InputPartition;

public class TalariaPartition implements InputPartition{
    public final String host;
    public final int port;
    public final String partitionBy;
    TalariaPartition(String host, int port, String partitionBy) {
       this.host = host;
       this.port = port;
       this.partitionBy = partitionBy;
    }
}