package com.talaria.spark.sql;


import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class TalariaSource implements DataSourceRegister, TableProvider {

    @Override
    public String shortName() {
        return "talaria";
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        Transform[] partitioning = {};
        return getTable(null, partitioning, options.asCaseSensitiveMap()).schema();
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        ReadOptions rc = new ReadOptions(properties);
        return new TalariaTable(rc);
    }

    @Override
    public boolean supportsExternalMetadata() {
        return true;
    }
}