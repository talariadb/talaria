package com.talaria.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class BatchExample {

    public static void main (String[] args){
        SparkSession spark = SparkSession.builder().master("local[*]").appName("Simple Application").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> d = spark.read()
                .format("com.talaria.spark.sql.TalariaSource")
                .option("tableName", "events")
                .option("partitionBy", "event == 'relay.outcome'")
                .load();
        d.printSchema();
        d.cache();
        d.groupBy("ingested_by").count().show();
        d.select("ingested_at").show(10);
        System.out.println(d.count());
        //d.collectAsList();

        spark.stop();

    }
}