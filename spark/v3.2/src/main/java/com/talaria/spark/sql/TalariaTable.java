package com.talaria.spark.sql;

import com.google.common.collect.ImmutableSet;
import com.talaria.client.TalariaClient;
import com.talaria.protos.ColumnMeta;
import com.talaria.protos.TableMeta;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;


public class TalariaTable implements Table, SupportsRead {

    private SparkSession lazySpark;
    private final ReadOptions readOptions;
    private String hashBy;
    private String sortBy;
    public TalariaTable(ReadOptions rc) {
        this.readOptions = rc;
        this.hashBy = "";
        this.sortBy = "";
    }

    @Override
    public String name() {
        return this.readOptions.getTable();
    }

    public ReadOptions getReadOptions() {
        return this.readOptions;
    }

    @Override
    public StructType schema() {
        TalariaClient tc = new TalariaClient(this.readOptions.getDomain(), this.readOptions.getPort());
        TableMeta tableMeta = tc.getTableMeta(this.readOptions.getTable());
        List<StructField> columns = new ArrayList<>();
        hashBy = tableMeta.getHashby();
        sortBy = tableMeta.getSortby();

        for (var i=0;i<tableMeta.getColumnsCount();i++){
            ColumnMeta column = tableMeta.getColumns(i);
                columns.add(new StructField(column.getName(), getColType(column.getType()), true, Metadata.empty()));
        }
        columns.sort(Comparator.comparing(StructField::name));
        tc.close();

        return new StructType(columns.toArray(new StructField[0]));
    }

    @Override
    public Set<TableCapability> capabilities() {
        return ImmutableSet.of(TableCapability.BATCH_READ, TableCapability.MICRO_BATCH_READ, TableCapability.CONTINUOUS_READ);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new TalariaScanBuilder(sparkSession(), this);
    }

    private SparkSession sparkSession(){
        if (lazySpark == null) {
            this.lazySpark = SparkSession.active();
        }
        return lazySpark;
    }

    public String getHashBy(){
        return this.hashBy;
    }

    public String getSortBy(){
        return this.sortBy;
    }

    public DataType getColType(String type) {
        switch (type) {
            case "JSON":
            case "VARCHAR":
            default:
                return DataTypes.StringType;
            case "BIGINT":
                return DataTypes.LongType;
            case "DOUBLE":
                return DataTypes.FloatType;
            case "TIMESTAMP":
                //return DataTypes.TimestampType;
                return DataTypes.LongType;
            case "BOOLEAN":
                return DataTypes.BooleanType;
        }
    }
}