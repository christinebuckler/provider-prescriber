import os, re, time
import pandas as pd
import pyspark as ps
from pyspark.sql import DataFrameWriter
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, ArrayType, DoubleType
from pyspark.sql.functions import *
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, MinHashLSH, BucketedRandomProjectionLSH
from pyspark.ml import Pipeline
from pyspark.mllib.linalg.distributed import RowMatrix

#os.environ["SPARK_CLASSPATH"] = '~/postgresql-9.1-901-1.jdbc4.jar'


if __name__ == '__main__':

    spark = ps.sql.SparkSession.builder \
                .appName("capstone") \
                .getOrCreate()

    cdf = spark.read.csv('npidata_20050523-20170813_clean.csv', \
                            header=True, inferSchema=True).limit(1000)

    rdd = cdf.rdd
    npi = rdd.map(lambda x: x[0])
    features = rdd.map(lambda x: x[1:])
    feature_cols = cdf.columns[1:]

    va = VectorAssembler(inputCols=feature_cols, outputCol='features')
    cdf = va.transform(cdf)

    mh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=10, seed=123)
    model = mh.fit(cdf)
    cdf = model.transform(cdf)

    distances = model.approxSimilarityJoin(cdf, cdf, .5, distCol='JaccardDistance')
    distances = distances.withColumn('NPI', distances.datasetA.NPI)
    distances = distances.withColumn('NPI_similar', distances.datasetB.NPI)
    distances = distances.drop('datasetA', 'datasetB')

    # write locally
    # distances.select('NPI', 'NPI_similar', 'JaccardDistance') \
    #             .write.csv('distances', header=True, mode='overwrite')

    # write remotely to S3
    distances.select('NPI', 'NPI_similar', 'JaccardDistance') \
                .write.csv('s3n://gschoolcapstone/distances', header=True, mode='overwrite')

    # write to Postgres database
    # format jdbc:postgresql://host:port/database
    # url = "jdbc:postgresql://128.177.113.102:5432/capstone"
    # table = "distances10000"
    # mode = "overwrite" # or "append"
    # properties = {"user":"postgres", "password":"postgres", "driver": "org.postgresql.Driver"}
    #
    # my_writer = DataFrameWriter(distances)
    # my_writer.jdbc(url=url, table='distances10000', mode=mode, properties=properties)
    # distances.write.jdbc(url=url, table="similarity", mode=mode, properties=properties)
