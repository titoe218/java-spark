package com.titoe.sparkexample;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JavaStreamStaticJoin {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("spark://spark-master:7077").appName("JavaConnectHDFS").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> df = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "hdfs-streaming")
                .option("auto.offset.reset", "latest")
                .option("enable.auto.commit", false)
                .load();

        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
        df.show();


    }
}
