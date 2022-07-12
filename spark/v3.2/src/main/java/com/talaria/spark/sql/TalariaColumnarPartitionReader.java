package com.talaria.spark.sql;

import com.google.protobuf.ByteString;
import com.talaria.client.TalariaClient;
import com.talaria.protos.Column;
import com.talaria.protos.GetRowsResponse;
import com.talaria.protos.Split;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TalariaColumnarPartitionReader implements PartitionReader<ColumnarBatch> {
    TalariaClient tc;
    GetRowsResponse rowsData;
    String tableName;
    StructType schema;

    ByteString splitID;
    List<String> columns;
    String paritionHost;
    Integer port;
    String partitionBy;

    TalariaColumnarPartitionReader(String partitionHost, Integer port, String partitionBy, String tableName, StructType schema){
        this.paritionHost = partitionHost;
        this.port = port;
        tc = new TalariaClient(partitionHost, port);
        this.tableName = tableName;
        this.schema = schema;
        this.partitionBy = partitionBy;
        String sortRangeFilter = "ingested_at >= 1655674700 && ingested_at < 1655674800";
        //String sortRangeFilter = "ingested_at >= 1655656870 && ingested_at < 1655658720";
        List<ByteString> splits = tc.getSplits(tableName, partitionBy);
        //List<ByteString> splits = tc.getSplits(tableName, partitionBy, sortRangeFilter);
        if (splits.size() == 0) {
            this.splitID = null;
        }else {
            this.splitID = splits.get(0);
        }
        this.columns = SparkUtil.getColumnsFromSchema(schema);
    }

    @Override
    public boolean next() {
        System.out.println(splitID);
        return splitID != null && !splitID.isEmpty();
    }

    @Override
    public ColumnarBatch get() {
        rowsData = tc.getRows(splitID, columns);
        List<ColumnVector> cols = new ArrayList<>();
        int rowCount = rowsData.getRowCount();
        rowsData.getColumnsList().forEach(col -> cols.add(SparkUtil.createColumnVector(rowCount, col)));
        ColumnVector[] cvs = cols.toArray(new ColumnVector[0]);
        ColumnarBatch batch = new ColumnarBatch(cvs, rowCount);
        splitID = rowsData.getNextToken();
        return batch;
    }

    @Override
    public void close() {
        tc.close();
    }



}