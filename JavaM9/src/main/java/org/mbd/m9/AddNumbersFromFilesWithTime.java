package org.mbd.m9;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class AddNumbersFromFilesWithTime {
    public static void main( String[] args )
    {
        Logger.getLogger("org").setLevel(Level.OFF);
        // Step 1. Create a SparkConf object, takes 4 cores
        SparkConf sparkConf = new SparkConf()
                .setAppName("Add numbers from files")
                .setMaster("local[8]");

        // Step 2. Create a Java Spark context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        double startTime = System.currentTimeMillis();

        int sum = sparkContext.textFile("data/manyNumbers.txt")
                .map(line -> Integer.valueOf(line))
                .reduce((a,b) -> a+b);

        double totalTime = System.currentTimeMillis() - startTime;

        // Step 6. Print the sum
        System.out.println("The sum is: " + sum);
        System.out.println("Computing time: " + totalTime);

        // Step 7. Stop Spark context
        sparkContext.stop();
    }
}
