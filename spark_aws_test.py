import pyspark as ps
import pandas as pd
import os, boto
# import s3fs
import random

# obtain the AWS credentials from system
ACCESS_KEY = os.environ['AWS_ACCESS_KEY_ID']
SECRET_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

def sample(p):
    x, y = random.random(), random.random()
    return 1 if x*x + y*y < 1 else 0


if __name__ == '__main__':
    # we try to create a SparkSession to work locally
    spark = ps.sql.SparkSession.builder \
            .master("local[3]") \
            .appName("MyApp") \
            .getOrCreate()

    # Grab sparkContext from the SparkSession object
    sc = spark.sparkContext
    sc.setLogLevel('ERROR')

    '''Test: run with pyspark'''
    # print("Creating RDD...")
    # random.seed(1)
    # count_rdd = sc.parallelize(range(0, 10000000)).map(sample) \
    #         .reduce(lambda a, b: a + b)
    # print("Pi is (very) roughly {}".format(4.0 * count_rdd / 10000000))

    '''Test: read sample data from S3 bucket'''
    # link = 's3n://{}:{}@mortar-example-data/airline-data'.format(ACCESS_KEY, SECRET_KEY)
    # airline_rdd = sc.textFile(link)
    # print(airline_rdd.take(2))

    '''Test: read sample data from my S3 bucket'''
    # df = pd.read_csv('https://s3.amazonaws.com/gschoolcapstone/iris_data.csv')
    # print(df.head(2))

    '''Test: read project data from my S3 bucket'''
    link = 's3n://{}:{}@gschoolcapstone/npidata_20050523-20170813.csv'.format(ACCESS_KEY, SECRET_KEY)
    rdd = sc.textFile(link)
    # print(rdd.take(2))
    print(rdd.count())

    sc.stop()
