package com.talaria.spark.sql;

import java.util.Map;

public class ReadOptions {
    private final String domain;
    private final Integer port;
    private final String schema;
    private final String table;
    private final String partitionFilter;
    private final String checkpointLocation;
    private final Long fromTimestamp;
    private final Long untilTimestamp;

    public String getDomain() {
        return domain;
    }

    public Integer getPort() {
        return port;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public String getPartitionFilter() {
        return partitionFilter;
    }

    public Long getFromTimestamp() {
        return fromTimestamp;
    }

    public Long getUntilTimestamp() {
        return untilTimestamp;
    }

    public String getCheckpointLocation() {
        return checkpointLocation;
    }

    public ReadOptions(Map<String, String> options) {
        String DOMAIN = "domain";
        String PORT = "port";
        String TABLE = "table";
        String SCHEMA = "schema";
        String PARTITIONFILTER = "partitionFilter";
        String CHECKPOINTLOCATION = "checkpointLocation";
        String FROMTS = "fromTimestamp";
        String UNTILTS = "untilTimestamp";
        this.domain = options.get(DOMAIN);
        this.port = Integer.valueOf(options.get(PORT));
        this.table = options.get(TABLE);
        this.schema = options.get(SCHEMA);
        this.partitionFilter = options.get(PARTITIONFILTER);
        this.fromTimestamp = parseTimeBoundOptions(options.get(FROMTS), true);
        this.untilTimestamp = parseTimeBoundOptions(options.get(UNTILTS), false);
        this.checkpointLocation = options.get(CHECKPOINTLOCATION);
    }

    private Long parseTimeBoundOptions(String epochStr, Boolean lower){
        long value = 0L;
        try {
            value = Long.parseLong(epochStr);
        }
        catch (NumberFormatException exception){
           if (!lower){
               value = Long.MAX_VALUE;
           }
        }
        return value;
    }
}