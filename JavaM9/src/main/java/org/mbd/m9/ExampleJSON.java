package org.mbd.m9;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ExampleJSON {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);

        SparkSession sparkSession =
                SparkSession.builder()
                        .appName("Analysis of airports data")
                        .master("local[4]")
                        .getOrCreate();

        Dataset<Row> dataFrame = sparkSession
                .read()
                .json("data/primer-dataset.json");

        dataFrame.printSchema();
        dataFrame.show(10);

        sparkSession.stop();
    }
}
