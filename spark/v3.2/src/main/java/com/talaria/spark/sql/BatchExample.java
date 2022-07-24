package com.talaria.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class BatchExample {

    public static void main (String[] args){
        SparkSession spark = SparkSession.builder().master("local[*]").appName("Simple Application").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> d = spark.read()
                .format("talaria")
                .option("domain", "127.0.0.1")
                .option("port", "8043")
                .option("schema", "data")
                .option("table", "events")
                .option("partitionFilter", "relay.outcome")
                .option("fromTimestamp", "1658634750")
                .load();
        d.printSchema();
        d.cache();
        d.groupBy("ingested_by").count().show();
        d.select("ingested_at").show(10);

        spark.stop();
    }
}