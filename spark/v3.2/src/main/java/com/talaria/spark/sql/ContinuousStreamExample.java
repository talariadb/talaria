package com.talaria.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.Trigger;

public class ContinuousStreamExample {
    public static void main (String[] args) throws Exception {

        SparkSession spark = SparkSession.builder().master("local[*]").appName("Simple Application").getOrCreate();
        Dataset<Row> df = spark.readStream()
                .format("com.talaria.spark.sql.TalariaSource")
                .option("tableName", "events")
                .option("partitionBy", "event == 'relay.outcome'")
                .load();
        df.printSchema();
        df.select("ingested_by", "bucket")
                .writeStream()
                .outputMode("append")
                .option("truncate", false)
                .format("console")
                .trigger(Trigger.Continuous("5 second"))  // only change in query
                .start()
                .awaitTermination(60000);
    }
}



