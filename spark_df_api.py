from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, DoubleType, IntegerType


# initialize spark session
def initialize_spark_session():
    return SparkSession.builder.getOrCreate()


def read_schemas(spark_instance, path_to_file):
    labels = [('ID', StringType()),
              ('Case Number', StringType()),
              ('Date', TimestampType()),
              ('Block', StringType()),
              ('IUCR', StringType()),
              ('Primary Type', StringType()),
              ('Description', StringType()),
              ('Location Description', StringType()),
              ('Arrest', StringType()),
              ('Domestic', BooleanType()),
              ('Beat', StringType()),
              ('District', StringType()),
              ('Ward', StringType()),
              ('Community Area', StringType()),
              ('FBI Code', StringType()),
              ('X Coordinate', StringType()),
              ('Y Coordinate', StringType()),
              ('Year', IntegerType()),
              ('Updated On', StringType()),
              ('Latitude', DoubleType()),
              ('Longitude', DoubleType()),
              ('Location', StringType())]

    print(labels)
    schema = StructType([StructField(x[0], x[1], True) for x in labels])
    df = spark_instance.read.csv(path_to_file, header=True, schema=schema)
    df.printSchema()
    df.show(5)
    return df


def working_with_columns(df):
    print("working with columns")
    # following three line show the same information
    df.select('IUCR').show(5)
    print('\n')
    df.select(df.IUCR).show(5)
    print('\n')
    df.select(df.IUCR).show(5)
    print('\n')
    # display the first 4 rows of the columns: Case Number, Date and Arrest
    df.select('Case Number', 'Date', 'Arrest').show(4)
    # create a column with name 'One' and all values 1
    df.withColumn('One', lit(1)).show(5)
    # remove the col 'IUCR'
    df.drop('IUCR').show(5)  # save to var: df if you want to save option


def working_with_rows(df):
    print("working with rows")
    # add the reported crimes for an additional day, 12 nov 2018
    df.groupBy('Primary Type').count().show()
    print('\n')


def challenge(df):
    print("working with challenge")

    # find the % of reported crimes that resulted in an arrest

    # step1: ensure that Arrest is true or false only - found: true,false
    df.select('Arrest').distinct().show()
    # step2: find the dtype of 'Arrest' - found: 'stringType'
    df.printSchema()
    # total # of arrests divided by total # of crimes gives us % of crimes that resulted in an arrest
    per = df.filter(col('Arrest') == 'true').count() / df.select('Arrest').count()
    print(per)

    # find the top 3 locations for reported crimes

    temp = df.groupBy('Location Description').count()
    temp.show()
    temp = temp.orderBy('count', ascending=False)
    temp.show(3)


if __name__ == '__main__':
    path = 'chicago_crimes.csv'
    spark = initialize_spark_session()
    rc = read_schemas(spark, path)
    working_with_columns(df=rc)
    working_with_rows(df=rc)
    challenge(df=rc)
