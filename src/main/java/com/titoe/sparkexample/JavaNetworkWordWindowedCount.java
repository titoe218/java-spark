package com.titoe.sparkexample;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class JavaNetworkWordWindowedCount {
    public static void main(String[] args) throws StreamingQueryException {
        String host = "localhost";
        int port = 9999;
        int windowSize = 10; // window 10 seconds
        int slideSize = 5; // slide 5 seconds
        String windowDuration = windowSize + " seconds";
        String slideDuration = slideSize + " seconds";

        SparkSession spark = SparkSession.builder().master("local[2]").appName("JavaNetworkWordWindowedCount").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> lines = spark.readStream()
                .format("socket")
                .option("host", host)
                .option("port", port)
                .option("includeTimestamp", true)
                .load();

        lines.isStreaming(); // return true for dataframes that have streaming sources
        lines.printSchema();

        // Split the lines into words, retaining timestamps
        Dataset<Row> words = lines
                .as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()))
                .flatMap((FlatMapFunction<Tuple2<String, Timestamp>, Tuple2<String, Timestamp>>) t -> {
                            List<Tuple2<String, Timestamp>> result = new ArrayList<>();
                            for (String word : t._1.split(" ")) {
                                result.add(new Tuple2<>(word, t._2));
                            }
                            return result.iterator();
                        },
                        Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP())
                ).toDF("word", "timestamp");

        // Group the data by window and word and compute the count of each group
        Dataset<Row> windowedCounts = words
                .withWatermark("timestamp", windowDuration)
                .groupBy(
                functions.window(words.col("timestamp"), windowDuration, slideDuration),
                words.col("word")
        ).count().orderBy("window");
        //  functions.window(words.col("timestamp"), windowDuration, slideDuration),

        // Start running the query that prints the windowed word counts to the console
        StreamingQuery query = windowedCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .option("truncate", "false")
                .start();
        
        query.awaitTermination();
    }
}
