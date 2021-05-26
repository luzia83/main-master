from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import Correlation, Summarizer
from pyspark.mllib.stat import Statistics
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark_session = SparkSession \
        .builder \
        .getOrCreate()

    logger = spark_session._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    data_list = [(Vectors.sparse(4, [(0, 1.0), (3, -2.0)]),),
            (Vectors.dense([4.0, 5.0, 0.0, 3.0]),),
            (Vectors.dense([6.0, 7.0, 0.0, 8.0]),),
            (Vectors.sparse(4, [(0, 9.0), (3, 1.0)]),)]

    data_frame = spark_session.createDataFrame(data_list, ["features"])
    data_frame.printSchema()
    data_frame.show()

    r1 = Correlation.corr(data_frame, "features").head()
    print("Pearson correlation matrix:\n" + str(r1[0]))

    r2 = Correlation.corr(data_frame, "features", "spearman").head()
    print("Spearman correlation matrix:\n" + str(r2[0]))

    rdd_data = data_frame.rdd
    print(rdd_data.collect())

    more_data = spark_session.sparkContext.parallelize([
        (Vectors.sparse(4, [(0, 1.0), (3, -2.0)])),
            (Vectors.dense([4.0, 5.0, 0.0, 3.0])),
            (Vectors.dense([6.0, 7.0, 0.0, 8.0])),
            (Vectors.sparse(4, [(0, 9.0), (3, 1.0)]))])

    rdd_data = more_data.map(lambda line: tuple([float(x) for x in line]))
    summary = Statistics.colStats(rdd_data)

    print("Mean:" + str(summary.mean()))  # a dense vector containing the mean value for each column
    print("Variance:" + str(summary.variance()))  # column-wise variance
    print("Non zeros:" + str(summary.numNonzeros()))
    print("Count:" + str(summary.count()))
    print("Min:" + str(summary.min()))
    print("Max:" + str(summary.max()))


    # Examples with Summarizer
    summarizer = Summarizer.metrics("mean", "count", "min", "max", "variance")

    # compute statistics for multiple metrics
    data_frame.select(summarizer.summary(data_frame.features)).show(truncate=False)

    # compute statistics for single metric "mean"
    data_frame.select(Summarizer.mean(data_frame.features)).show(truncate=False)

    spark_session.stop()





