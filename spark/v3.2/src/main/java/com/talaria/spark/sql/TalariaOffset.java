package com.talaria.spark.sql;

import org.apache.spark.sql.connector.read.streaming.Offset;
import org.json.JSONObject;

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

    public static TalariaOffset fromJSON(String json){
        JSONObject jsonObject = new JSONObject(json);
        return new TalariaOffset(jsonObject.getLong("offset"));
    }
}