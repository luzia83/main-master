package org.mbd.m9;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AddNumbersFromFileCompact {
    public static void main( String[] args )
    {
        Logger.getLogger("org").setLevel(Level.OFF);
        // Step 1. Create a SparkConf object, takes 4 cores
        SparkConf sparkConf = new SparkConf()
                .setAppName("Add numbers from files")
                .setMaster("local[4]");

        // Step 2. Create a Java Spark context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        int sum = sparkContext.textFile("data/numbers.txt")
                .map(line -> Integer.valueOf(line))
        //        .filter(number -> number%2 == 0)
                .reduce((a,b) -> a+b);

        // Step 6. Print the sum
        System.out.println("The sum is: " + sum);

        // Step 7. Stop Spark context
        sparkContext.stop();
    }
}
