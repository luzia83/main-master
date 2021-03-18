package org.mbd.m9;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.ColumnName;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.storage.StorageLevel;
import javax.xml.crypto.Data;
import java.util.List;

public class BasicOperations {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);

        SparkSession sparkSession =
                SparkSession.builder()
                    .appName("Java Spark SQL basic example")
                    .master("local[8]")
                    .getOrCreate();

        Dataset<Row> dataFrame = sparkSession
                .read()
                .format("csv")
                .option("header", "true")
                .load("data/simple.csv");

        dataFrame.printSchema();
        dataFrame.show();

        System.out.println("Data types: " + dataFrame.dtypes());

        dataFrame.describe().show();
        dataFrame.explain();

        //Select the names
        dataFrame.select("Name").show();

        //Select columns name and age
        dataFrame.select(col("Name"), col("Age")).show();

        //Select columns name and age but adding 2 to age
        dataFrame.select(col("Name"), col("Age").plus(2)).show();

        //Select the rows having a name length > 4
        dataFrame.select(functions.length(col("Name")).gt(4)).show();

        //Select names starting with "L"
        dataFrame
                .select(col("Name"), col("Name").startsWith("L"))
                .withColumnRenamed("startsWith(Name, L)", "L-names")
                .show();

        //Select names starting with "L"
        dataFrame.select(col("Name"), col("Name").startsWith("L")).show();

        //Add a new column name "Senior" containig true if age > 50
        dataFrame.withColumn("Senior", col("Age").gt(50)).show();

        //Rename HasACar as Owner
        dataFrame.withColumnRenamed("HasACar", "Owner").show();

        //Remove a column
        dataFrame.drop("BirthDate").show();

        //Sort by age
        dataFrame.sort(col("Age").desc()).show();
        dataFrame.orderBy(col("Age").desc(), col("Weight")).show();

        //Get a RDD from a DataFrame
        JavaRDD<Row> rddFromDataFrame = dataFrame.javaRDD().persist(StorageLevel.MEMORY_AND_DISK());

        for (Row row : rddFromDataFrame.collect()) {
            System.out.println(row);
        }

        //sum all weights (RDDs)
        double sumOfWeights = rddFromDataFrame
                .map(row -> Double.valueOf((String) row.get(2)))
                .reduce((x,y) -> x+y);

        System.out.println("Sum of weights (RDD): " + sumOfWeights);

        // sum all weights (dataframes)
        dataFrame.select("Weight").groupBy().sum().show();
        dataFrame.select(functions.sum("Weight")).show();
        dataFrame.agg(sum("Weight")).show();

        System.out.println("Sum of weights (RDD): " +
                dataFrame.agg(sum("weight")).collectAsList().get(0).get(0));

        //Compute the mean age (RDDs)
        int sumOfAges = rddFromDataFrame
                .map(row -> Integer.valueOf((String) row.get(1)))
                .reduce((x,y) -> x+y);

        System.out.println("Mean age: " + sumOfAges/rddFromDataFrame.count());

        //Compute the mean age (dataframe)
        dataFrame
                .select(functions.avg("Age"))
                .withColumnRenamed("avg(Age)", "Average")
                .show();

        //write to a csv file
        dataFrame.write().csv("output.csv");

        //Write to a JSON file
        dataFrame.write().json("output.json");

        sparkSession.stop();
    }
}

