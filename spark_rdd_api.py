import findspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, DoubleType, IntegerType


# we will try to understand Resilient Distributed Dataset (RDD) API for Apache Spark


# initialize spark session or RDD
def initialize_spark_session():
    return SparkSession.builder.getOrCreate()


def get_rdd(spark_instance, path_to_file):
    return spark_instance.read.text(path_to_file)


def assign_col_names(rdd):
    ps_header = ps_rdd.first()
    return rdd.filter(lambda line: line != ps_header)


if __name__ == '__main__':
    path = 'chicago_police_stations.csv'
    spark = initialize_spark_session()
    ps_rdd = get_rdd(spark_instance=spark, path_to_file=path)
    ps_rdd = assign_col_names(rdd=ps_rdd)

