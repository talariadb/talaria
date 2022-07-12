package com.talaria.spark.sql;

import java.util.Map;

public class ReadOptions {
    private final String domain;
    private final Integer port;
    private final String schema;
    private final String table;
    private final String partitionFilter;
    private final String checkpointLocation;

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
        this.domain = options.get(DOMAIN);
        this.port = Integer.valueOf(options.get(PORT));
        this.table = options.get(TABLE);
        this.schema = options.get(SCHEMA);
        this.partitionFilter = options.get(PARTITIONFILTER);
        this.checkpointLocation = options.get(CHECKPOINTLOCATION);
    }
}
