package org.mbd.m9;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class AirportDataframesSQL {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);

        SparkSession sparkSession =
                SparkSession.builder()
                        .appName("Java Spark SQL basic example")
                        .master("local[8]")
                        .getOrCreate();

        Dataset<Row> dataFrame =
                sparkSession.read()
                        .format("csv")
                        .option("header", "true")
                        .load("data/airports.csv");

        dataFrame.printSchema();
        dataFrame.show();

        dataFrame.createOrReplaceTempView("airports");

        Dataset<Row> spanishAirports = sparkSession
                .sql("SELECT iso_country, type FROM airports WHERE iso_country == \"ES\"");
        spanishAirports.printSchema();

        spanishAirports.groupBy("type").count().show();
    }
}