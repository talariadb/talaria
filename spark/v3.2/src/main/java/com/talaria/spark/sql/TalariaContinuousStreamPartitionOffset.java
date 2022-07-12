package com.talaria.spark.sql;

import org.apache.spark.sql.connector.read.streaming.PartitionOffset;

public class TalariaContinuousStreamPartitionOffset implements PartitionOffset {
    long offset;
    TalariaContinuousStreamPartitionOffset(long offsetValue) {
        this.offset = offsetValue;
    }
}
