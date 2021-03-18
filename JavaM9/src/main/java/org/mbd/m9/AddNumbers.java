package org.mbd.m9;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;
import java.util.List;

/**
 * Program that sums a list of numbers using Spark's RDD API
 */
public class AddNumbers
{
    public static void main( String[] args )
    {
        Logger.getLogger("org").setLevel(Level.OFF);
        // Step 1. Create a SparkConf object, takes 4 cores
        SparkConf sparkConf = new SparkConf().setAppName("Add numbers").setMaster("local[4]");

        // Step 2. Create a Java Spark context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // Step 3. Create a list of integer numbers
        Integer[] numbers = new Integer[]{1,2,3,4,5,6,7,8};
        List<Integer> integerList = Arrays.asList(numbers);

        // o tambien
        //List<Integer> integerList = List.of(1,2,3,4,5,6,7,8);

        //int sum = 0;
        //for(Integer integer: integerList) {
        //    sum = sum + integer;
        //}

        //Step 4. Create a JavaRDD
        JavaRDD<Integer> distributedList = sparkContext.parallelize(integerList);

        // Step 5. Sum the numbers
        LongAccumulator counter = sparkContext.sc().longAccumulator();

    //    int sum = distributedList.reduce((number1, number2) -> number1 + number2);
        //dentro de las operacion de spark como reduce no se pueden modificar variables, excepto contadores o acumuladores
        int sum = distributedList.reduce((number1, number2) -> {
            counter.add(1);
            return number1 + number2; });

        // Step 6. Print the sum
        System.out.println("The sum is: " + sum);

        // Step 7. Stop Spark context
        sparkContext.stop();
    }
}
