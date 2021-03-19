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

public class FilmLocationsSanFrancisco {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);

        // Step 1. Create a SparkConf object, takes 4 cores
        SparkConf sparkConf = new SparkConf()
                .setAppName("Add numbers from files")
                .setMaster("local[4]");

        // Step 2. Create a Java Spark context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // Step 3. Read the file contents
        JavaRDD<String> lines = sparkContext.textFile("data/Film_Locations_in_San_Francisco.csv");

        // Step 4. Read the fields
        JavaRDD<String[]> lineData = lines.map(line -> line.split(";"));
        JavaRDD<String> allFilmsNames = lineData.map(row -> row[0]);
        long numberFilms = allFilmsNames
                .distinct()
                .count();

        // Step 5. Filter rows to avoid execution time errors on some wrong lines
        JavaRDD<String[]> lineDataFiltered = lineData.filter(line -> line.length > 3);

        // Step 6: map operation to isolate name and location fields
        JavaPairRDD<String, String> filmNameLocation = lineDataFiltered
                .mapToPair(row -> new Tuple2<>(row[0], row[2]));

        // Step 7. Just consider distinct pairs name, location
        JavaPairRDD<String, String> filmNameLocationDistinct = filmNameLocation.distinct();

        // Step 8. Take the name of the film
        JavaRDD<String> filmName = filmNameLocationDistinct.map(row -> row._1());

        // STEP 9: map operation to create pairs <type, 1> per every row
        JavaPairRDD<String, Integer> pairs = filmName
                .mapToPair(row -> new Tuple2<>(row, 1));

        // Step 10. Reduce operation that sum the values of all the pairs having the same key, generating a pair <key, sum>
        JavaPairRDD<String, Integer> groupedPairs = pairs
                .reduceByKey((integer, integer2) -> integer + integer2);

        JavaRDD<Integer> numberLocations = groupedPairs.map(row -> row._2());

        long totalLocations = numberLocations.reduce((a, b) -> a + b);
        double avgLocations = (double)totalLocations/(double)numberFilms;

        // Step 11. Apply a filter to get those films with at least 20 locations
        JavaPairRDD<String, Integer> pairsResults = groupedPairs
                .filter(row -> row._2().intValue() >= 20);

        // Step 12. Map operation to get an RDD of pairs <sum, key> to sort by value (count)
        JavaPairRDD<Integer, String> reversedPairs = pairsResults
                .mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()));

        // Step 13: sort the results by key
        List<Tuple2<Integer, String>> results = reversedPairs
                .sortByKey(false)
                .collect();

        // STEP 11: print the results and save to a text file
        try {
            PrintWriter out = new PrintWriter("filmLocationsSanFrancisco.txt");

            for (Tuple2<?, ?> tuple : results) {
                System.out.println("(" + tuple._1() + ", " + tuple._2() + ")");
                out.println("(" + tuple._1() + ", " + tuple._2() + ")");
            }
            System.out.println("Total number of films: " + numberFilms);
            System.out.println("The average of film locations per film: " + avgLocations);
            out.println("Total number of films: " + numberFilms);
            out.println("The average of film locations per film: " + avgLocations);

            out.close();
        }catch(FileNotFoundException e) {
            System.err.println(e.getMessage());
        }

        // STEP 19: stop the spark context
        sparkContext.stop();
    }
}

