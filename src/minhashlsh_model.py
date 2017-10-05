import pandas as pd
import pyspark as ps
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler, MinHashLSH


if __name__ == '__main__':

    spark = ps.sql.SparkSession.builder \
                .appName("minhash") \
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

    distances.select('NPI', 'NPI_similar', 'JaccardDistance') \
                .write.csv('distances', header=True, mode='overwrite')

    # distances.select('NPI', 'NPI_similar', 'JaccardDistance') \
    #             .write.csv('s3n://gschoolcapstone/distances', header=True, mode='overwrite')
