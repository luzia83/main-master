from pyspark.sql import SparkSession, functions


def main() -> None:
    spark_session = SparkSession\
        .builder\
        .master("local[4]")\
        .getOrCreate()

    logger = spark_session._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    data_frame = spark_session \
        .read \
        .json("data/primer-dataset.json")

    data_frame.printSchema()
    data_frame.show()

    # selecciona los nombres de todos los restaurantes (columna "name")
    data_frame.select("name").show()

    # selecciona los nombres y tipo de cocina de todos los restaurantes (columnas "name" y "cuisine")
    data_frame.select("name", "cuisine").show()

    # filtra aquellos restaurantes de cocina tipo americana
    data_frame.filter(data_frame["cuisine"] == "American").show()

    # agrupa los restaurantes por barrio (columna "borough") y los suma
    data_frame.groupBy("borough") \
        .count() \
        .show()

    data_frame.createOrReplaceTempView("restaurants")

    sql_data_frame = spark_session.sql("SELECT cuisine FROM restaurants")
    sql_data_frame.show()

if __name__ == "__main__":
    main()
