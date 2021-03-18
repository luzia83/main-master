package org.mbd.m9;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class ReadCSV {
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

        //JavaRDD<String> lineData = lines.flatMap(line -> List.of(line.split(",")).iterator());
        JavaRDD<String[]> lineData = lines.map(line -> line.split(","));

        JavaRDD<String> airportNames = lineData.map(array -> array[3]);

        List<String> sortedNames = airportNames
                .sortBy(name -> name, true, 1)
                .collect();

        for (String name: airportNames.collect()) {
            System.out.println(name);
        }

        // STEP 19: stop the spark context
        sparkContext.stop();
    }
}
