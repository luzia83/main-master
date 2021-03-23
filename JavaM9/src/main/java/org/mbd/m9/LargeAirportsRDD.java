package org.mbd.m9;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.reflect.runtime.SynchronizedSymbols;

import java.util.List;

public class LargeAirportsRDD {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);

        // Step 1. Create a SparkConf object, takes 4 cores
        SparkConf sparkConf = new SparkConf()
                .setAppName("Large airports (RDD)")
                .setMaster("local[4]");

        // Step 2. Create a Java Spark context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // Step 3. Read the file contents
        JavaRDD<String> airports = sparkContext.textFile("data/airports.csv");
        JavaRDD<String> countries = sparkContext.textFile("data/countries.csv");

        JavaRDD<String[]> airportData = airports.map(line -> line.split(","));
        JavaRDD<String[]> countryData = countries.map(line -> line.split(","));

        JavaPairRDD<String, String> countryCode = countryData
                .mapToPair(line -> new Tuple2<>(line[1], line[2]));

        JavaPairRDD<String, Integer> airportCountry = airportData
                .filter(line -> line[2].equals("\"large_airport\""))
                .map(line -> line[8])
                .mapToPair(row -> new Tuple2<>(row, 1))
                .reduceByKey((a, b) -> a + b);

        List<Tuple2<Integer, String>> output = airportCountry
                .join(countryCode)
                .map(line -> line._2())
                .mapToPair(pair -> new Tuple2<>(pair._1(), pair._2()))
                .sortByKey(false)
                .take(10);

        System.out.println("+--------------+-----+");
        System.out.println("|          name|count|");
        System.out.println("+--------------+-----+");
        for (Tuple2<?, ?> tuple : output) {
            String pais = tuple._2().toString().replace("\"","");
            String count = tuple._1().toString();
            System.out.println("|" + String.format("%1$14s", pais) + "| " + String.format("%1$4s", count) + "|");
        }
        System.out.println("+--------------+-----+");

        sparkContext.stop();
    }
}