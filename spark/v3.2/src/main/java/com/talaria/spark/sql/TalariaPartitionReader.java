package com.talaria.spark.sql;

import com.google.protobuf.ByteString;
import com.talaria.client.TalariaClient;
import com.talaria.protos.GetRowsResponse;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.Arrays;
import java.util.List;


public class TalariaPartitionReader implements PartitionReader<InternalRow> {
    private final TalariaClient tc;
    private final GetRowsResponse rowsData;


    TalariaPartitionReader(String partitionHost, Integer port, String tableName, StructType schema){
        tc = new TalariaClient(partitionHost, port);
        ByteString splitID = tc.getSplits(tableName, "address != 'test'").get(0);
        this.rowsData = tc.getRows(splitID, getColumnsFromSchema(schema));
    }


    //String[] values = {"1"};

    int index = 0;

    @Override
    public boolean next() {
        return index < rowsData.getRowCount();
    }

    @Override
    public InternalRow get() {
        // TableMeta tableMeta = tc.getTableMetadata(tableName);
        UTF8String st = UTF8String.fromString("Manoj");
        InternalRow row = new GenericInternalRow(new UTF8String[] {st, st});
        index = index + 1;
        return row;
    }

    @Override
    public void close() {
        tc.close();
    }

    private List<String> getColumnsFromSchema(StructType schema) {
        return Arrays.asList(schema.fieldNames());
    }
}