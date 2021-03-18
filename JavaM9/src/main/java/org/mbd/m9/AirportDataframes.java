package org.mbd.m9;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class AirportDataframes {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);

        SparkSession sparkSession =
                SparkSession.builder()
                        .appName("Analysis of airports data")
                        .master("local[4]")
                        .getOrCreate();

        Dataset<Row> dataFrame = sparkSession
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("data/airports.csv")
                .cache();

        dataFrame.printSchema();
        dataFrame.show(10);

        dataFrame
                .groupBy("iso_country")
                .count()
                .sort(col("count").desc())
                .show();

        dataFrame
                .filter(col("iso_country").equalTo("ES"))
                .groupBy("type")
                .count()
                .sort(col("count").desc())
                .show();

        sparkSession.stop();
    }
}