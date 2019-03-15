package com.titoe.sparkexample;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JavaConnectHDFS {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("spark://spark-master:7077").appName("JavaConnectHDFS").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> usersDF = spark.read().format("avro").load("hdfs://namenode:8020/topics/hdfs-connector/partition=0/hdfs-connector+0+0000000000+0000000002.avro");
        usersDF.show();

    }
}
