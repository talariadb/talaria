package com.talaria.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.Trigger;

public class MicroBatchStreamExample {

    public static void main (String[] args) throws Exception{
        //TalariaClient tc = TalariaClient.getClient("127.0.0.1", 8043);
        //tc.getTableMetadata("events");
        SparkSession spark = SparkSession.builder().master("local[*]").appName("Simple Application").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
//        Dataset<Row> d = spark.read()
//                .format("com.talaria.spark.sql.TalariaSource")
//                .option("tableName", "events")
//                .option("partitionBy", "event == 'relay.outcome'")
//                .load();
//        d.printSchema();
//        d.cache();
//        d.groupBy(new Column("ingested_by")).count().show();
//        System.out.println(d.count());
//        d.select("ingested_at").show(1000);
//        //d.collectAsList();

        //spark.stop();

        Dataset<Row> df = spark.readStream()
                .format("com.talaria.spark.sql.TalariaSource")
                .option("domain", "127.0.0.1")
                .option("port", "8043")
                .option("schema", "data")
                .option("table", "events")
                .option("partitionFilter", "event == 'relay.outcome'")
                //.option("partitionBy", "event == 'talaria-stream.click'")
                .option("checkpointLocation", "gs://talaria_pubsub_stg/")
                .load();
        df.printSchema();
// Output the cumulative sum of the rows.
        df.groupBy("ingested_by").count()
           .writeStream()
           .outputMode("complete")
           .option("truncate", false)
           .format("console")
           .trigger(Trigger.ProcessingTime(10000))  // only change in query
           .start()
           .awaitTermination(60000*10);

// Output the rows as received.
//         df.select("ingested_at")
//            .writeStream()
//            .outputMode("append")
//            .option("truncate", false)
//            .format("console")
//            .trigger(Trigger.ProcessingTime(10000))  // only change in query
//            .start()
//            .awaitTermination(60000*10);

    }
}