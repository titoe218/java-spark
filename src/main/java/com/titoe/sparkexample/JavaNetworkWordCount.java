package com.titoe.sparkexample;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JavaNetworkWordCount {
    public static void main(String[] args) throws StreamingQueryException {
        String host = "localhost";
        int port = 9999;

        SparkSession spark = SparkSession.builder().master("local[2]").appName("JavaNetworkWordCount").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> lines = spark.readStream()
                .format("socket")
                .option("host", host)
                .option("port", port)
                .load();

        lines.isStreaming(); // return true for dataframes that have streaming sources
        lines.printSchema();

        Dataset<String> words = lines
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

        Dataset<Row> wordCounts = words.groupBy("value").count();

        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();
    }
}
