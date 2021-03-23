from pyspark.sql import SparkSession, functions


def main() -> None:
    spark_session = SparkSession\
        .builder\
        .master("local[4]") \
        .appName("Large airports (Dataframe)") \
        .getOrCreate()

    logger = spark_session._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    df_airports = spark_session \
        .read \
        .options(header='true', inferschema='true') \
        .option("delimiter", ",") \
        .csv("data/airports.csv") \
        .persist()

    df_countries = spark_session \
        .read \
        .options(header='true', inferschema='true') \
        .option("delimiter", ",") \
        .csv("data/countries.csv") \
        .persist()

    df_airports.printSchema()
    df_airports.show()

    df_airports\
        .filter(df_airports["type"] == "large_airport")\
        .drop("name")\
        .join(df_countries, df_airports["iso_country"] == df_countries["code"]) \
        .groupBy("name")\
        .count()\
        .sort("count", ascending=False)\
        .show(10)


if __name__ == '__main__':
    main()