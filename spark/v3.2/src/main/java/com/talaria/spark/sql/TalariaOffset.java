package com.talaria.spark.sql;

import org.apache.spark.sql.connector.read.streaming.Offset;

public class TalariaOffset extends Offset {

    private final Long offset;

    TalariaOffset(Long offset) {
        this.offset = offset;
    }

    @Override
    public String json() {
        return "{ \"offset\" :  " + offset +"}";
    }

    public Long getOffset() {
        return this.offset;
    }
}