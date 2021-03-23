from pyspark import SparkConf, SparkContext


def main() -> None:

    spark_conf = SparkConf().setAppName("Large airports (RDD)").setMaster("local[4]")
    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    airports = spark_context.textFile("data/airports.csv")
    countries = spark_context.textFile("data/countries.csv")

    airports_data = airports.map(lambda line : line.split(","))
    countries_data = countries.map(lambda line: line.split(","))

    country_code = countries_data.map(lambda line: (line[1], line[2]))

    airport_country = airports_data\
        .filter(lambda row : row[2] == "\"large_airport\"")\
        .map(lambda row : row[8])\
        .map(lambda value: (value, 1))\
        .reduceByKey(lambda a, b : a+b )

    result = airport_country\
        .join(country_code)\
        .map(lambda line: line[1])\
        .sortBy(lambda pair: pair[0], ascending=False)\
        .take(10)

    print("+--------------+-----+")
    print("|          name|count|")
    print("+--------------+-----+")
    for count, name in result:
        print("|{:>14}|{:>4}|".format(name.replace("\"",""),count))
    print("+--------------+-----+")


if __name__ == '__main__':
    main()