from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.sql import SparkSession
import numpy as np

if __name__ == "__main__":
    spark_session = SparkSession.builder\
        .appName("Data types")\
        .master("local[4]")\
        .getOrCreate()

    dense_vector = [1.0, 0.0, 3.2]
    dense_vector2 = np.array([1.0, 0.0, 3.2])
    dense_vector3 = Vectors.dense([1.0, 0.0, 3.2])

    sparse_vector = Vectors.sparse(3, [0, 2], [1.0, 3.2])

    labeled_point = LabeledPoint(1.0, dense_vector)
    labeled_point2 = LabeledPoint(0.0, Vectors.sparse(5, [2, 4], [5.2, 6.2]))
    labeled_point3 = LabeledPoint(0.0, dense_vector2)

    print("Vector 1 (Python list) : " + str(dense_vector))
    print("Vector 2 (NumPy Array) : " + str(dense_vector2))
    print("Vector 3 (Vectors) : " + str(dense_vector3))
    print("Vector 4 (Vectors): " + str(sparse_vector))

    print("Labeled point (Python list): " + str(labeled_point))
    print("Labeled point (Sparse vector): " + str(labeled_point2))
    print("Labeled point (Numpy vector): " + str(labeled_point3))

    spark_session.stop()