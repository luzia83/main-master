package org.mbd.m9;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class AddNumbersFromFiles {
    public static void main( String[] args )
    {
        Logger.getLogger("org").setLevel(Level.OFF);
        // Step 1. Create a SparkConf object, takes 4 cores
        SparkConf sparkConf = new SparkConf()
                .setAppName("Add numbers from files")
                .setMaster("local[1]");

        // Step 2. Create a Java Spark context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // Step 3. Read the file contents
        //JavaRDD<String> lines = sparkContext.textFile("data/numbers.txt");
        JavaRDD<String> lines = sparkContext.textFile("data/manyNumbers.txt");

        // Step 4. Get a RDD of Integers
        JavaRDD<Integer> numbers = lines.map(line -> Integer.valueOf(line)); //transformacion

        // Step 5. Sum the numbers
        int sum = numbers.reduce((number1, number2) -> number1 + number2); //accion

        // Step 6. Print the sum
        System.out.println("The sum is: " + sum);

        // Step 7. Stop Spark context
        sparkContext.stop();
    }
}
