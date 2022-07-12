package com.talaria.spark.sql;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.sources.Filter;

public class TalariaScanBuilder implements ScanBuilder, SupportsPushDownFilters {

    private final TalariaTable table;

    TalariaScanBuilder(TalariaTable table){
        this.table = table;
    }

    private Filter[] _pushedFilters;
    @Override
    public Scan build() {
        return new TalariaScan(table);
    }

    @Override
    public Filter[] pushFilters(Filter[] filters) {
        _pushedFilters = filters;
        return filters;
    }

    @Override
    public Filter[] pushedFilters() {
        return _pushedFilters;
    }
}