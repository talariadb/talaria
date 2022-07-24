package com.talaria.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.Trigger;

public class MicroBatchStreamExample {

    public static void main (String[] args) throws Exception{
        SparkSession spark = SparkSession.builder().master("local[*]")
                .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
                .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
                .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
                .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/Users/manojbabu/keys/terraform-tracking-stg.json")
                .appName("Simple Application").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        //spark.sparkContext().hadoopConfiguration().set("fs.defaultFS", "gs://airasia-tracking-stg-ingestor-raw");

        Dataset<Row> df = spark.readStream()
                .format("talaria")
                .option("domain", "127.0.0.1")
                .option("port", "8043")
                .option("schema", "data")
                .option("table", "events")
                .option("partitionFilter", "relay.outcome")
                //.option("partitionBy", "event == 'talaria-stream.click'")
                //.option("checkpointLocation", "file:///Users/manojbabu/go/src/github.com/kelindar/talaria/offset")
                .option("checkpointLocation", "gs://airasia-tracking-stg-ingestor-raw/offset")
                .load();
        df.printSchema();
        df.groupBy("ingested_by").count()
           .writeStream()
           .outputMode("complete")
           .option("truncate", false)
           .format("console")
           .trigger(Trigger.ProcessingTime(10000))
           .start()
           .awaitTermination(60000*10);
    }
}