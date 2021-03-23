package org.mbd.m9;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class LargeAirportsDF {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);

        SparkSession sparkSession =
                SparkSession.builder()
                        .appName("Large airports (Dataframe)")
                        .master("local[4]")
                        .getOrCreate();

        Dataset<Row> dfAirports = sparkSession
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("data/airports.csv")
                .cache();

        dfAirports.printSchema();
        dfAirports.show(10);

        Dataset<Row> dfCountries = sparkSession
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("data/countries.csv")
                .cache();

        dfCountries.printSchema();
        dfCountries.show(10);

        dfAirports
                .filter(col("type").equalTo("large_airport"))
                .drop(col("name"))
                .join(dfCountries, dfAirports.col("iso_country").equalTo(dfCountries.col("code")))
                .groupBy("name")
                .count()
                .sort(col("count").desc())
                .show(10);

        sparkSession.stop();
    }
}
