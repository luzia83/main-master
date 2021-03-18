package org.mbd.m9;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.List;

public class SpanishAirports {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);

        // Step 1. Create a SparkConf object, takes 4 cores
        SparkConf sparkConf = new SparkConf()
                .setAppName("Add numbers from files")
                .setMaster("local[4]");

        // Step 2. Create a Java Spark context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // Step 3. Read the file contents
        JavaRDD<String> lines = sparkContext.textFile("data/airports.csv");

        // Step 4. Read the fields
        JavaRDD<String[]> lineData = lines.map(line -> line.split(","));

        // Step 5. Filter those with iso_country = ES
        JavaRDD<String[]> lineDataES = lineData.filter(array -> array[8].equals("\"ES\""));

        // Step 6. Take only field 'type'
        JavaRDD<String> airportTypes = lineDataES.map(array -> array[2]);

        // STEP 7: map operation to create pairs <type, 1> per every row
        JavaPairRDD<String, Integer> typePairs = airportTypes
                .mapToPair(row -> new Tuple2<>(row, 1));

        // STEP 8: reduce operation that sum the values of all the pairs having the same key, generating a pair <key, sum>
        JavaPairRDD<String, Integer> groupedPairs = typePairs
                .reduceByKey((integer, integer2) -> integer + integer2);

        // STEP 9: map operation to get an RDD of pairs <sum, key> to sort by value (count)
        JavaPairRDD<Integer, String> reversedPairs = groupedPairs
                .mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()));

        // STEP 10: sort the results by key
        List<Tuple2<Integer, String>> output = reversedPairs
                .sortByKey(false)
                .collect();

        // STEP 11: print the results and save to a text file
        try {
            PrintWriter out = new PrintWriter("spanishAirports.txt");

            for (Tuple2<?, ?> tuple : output) {
                System.out.println(tuple._2() + ": " + tuple._1());
                out.println(tuple._2() + ": " + tuple._1());
            }
            out.close();
        }catch(FileNotFoundException e) {
            System.err.println(e.getMessage());
        }

        // STEP 12: stop the spark context
        sparkContext.stop();
    }
}
