from pyspark.sql import SparkSession, functions


def main() -> None:
    spark_session = SparkSession\
        .builder\
        .master("local[8]")\
        .getOrCreate()

    logger = spark_session._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    data_frame = spark_session \
        .read \
        .options(header='true', inferschema='true') \
        .option("delimiter", ",") \
        .csv("data/simple.csv") \
        .persist()

    data_frame.printSchema()
    data_frame.show()

    # Print the data types of the data frame
    print("data types: " + str(data_frame.dtypes))

    # Describe the dataframe
    data_frame\
        .describe()\
        .show()

    # Explain the dataframe
    data_frame \
        .explain()

    # select the names
    data_frame.select("Name").show()

    # select the columns name and age
    data_frame.select("Name", "Age").show()

    # select columns name and age but adding 2 to age
    data_frame.select("Name", data_frame["Age"] + 2).show()

    # select rows having a name length > 4
    data_frame.select(functions.length("Name") > 4).show()

    # select names starting with L
    data_frame\
        .select("Name", data_frame["Name"].startswith("L"))\
        .withColumnRenamed("startswith(Name,L)", "L-Name")\
        .show()

    # add a new column name "senior"
    data_frame.withColumn("Senior", data_frame["Age"] > 50).show()

    # rename HasACar to Owner
    data_frame.withColumnRenamed("HasACar", "Owner").show()

    # remove a column
    data_frame.drop("BirthDate").show()

    # sort by age
    data_frame.sort("Age").show()
    data_frame.sort(data_frame["Age"].desc()).show()

    # Get a RDD
    rdd_from_dataframe = data_frame\
        .rdd\
        .cache()

    for i in rdd_from_dataframe.collect():
        print(i)

    # Sum all the weights (RDD)
    sum_of_weights = rdd_from_dataframe\
        .map(lambda row: row[2])\
        .reduce(lambda x, y: x + y)
    print("Sum of weights (RDDs): " + str(sum_of_weights))

    v = data_frame.select("Weight").groupBy().sum().collect()
    print(v)
    print(v[0][0])

    # Sum all the weights (dataframe)
    weights = data_frame \
        .select("Weight") \
        .groupBy() \
        .sum() \
        .collect()

    print(weights)
    print("Sum of weights (dataframe): " + str(weights[0][0]))

    data_frame.select(functions.sum(data_frame["Weight"])).show()
    data_frame.agg({"Weight": "sum"}).show()

    # Get the mean age (RDD)
    total_age = rdd_from_dataframe\
        .map(lambda row: row[1]) \
        .reduce(lambda x, y: x + y)

    mean_age = total_age / rdd_from_dataframe.count()

    print("Mean age (RDDs): " + str(mean_age))

    # Get the mean age (dataframe)
    data_frame.select(functions.avg(data_frame["Weight"])) \
        .withColumnRenamed("avg(Weight)", "Average") \
        .show()

    data_frame.agg({"Weight": "avg"}).show()

    # Write to a json file
    data_frame\
        .write\
        .save("output.json", format="json")

    # Write to a CSV file
    data_frame\
        .write\
        .format("csv")\
        .save("output.csv")

if __name__ == "__main__":
    main()
