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
        .csv("data/airports.csv") \
        .persist()

    data_frame.printSchema()
    data_frame.show()

    data_frame\
        .groupBy("iso_country")\
        .count()\
        .sort("count", ascending = False)\
        .show()

    data_frame \
        .filter(data_frame["iso_country"].contains("ES")) \
        .groupBy("type") \
        .count()\
        .sort("count", ascending=False) \
        .show()

if __name__ == "__main__":
    main()
