from pyspark.sql import SparkSession

if __name__ == "__main__":
    sparkSession = SparkSession \
        .builder \
        .getOrCreate()

    dataset = sparkSession \
        .read \
        .format("libsvm") \
        .load("data/simple_libsvm_data.txt")

    dataset.printSchema()
    dataset.show(truncate=False)

    sparkSession.stop()
