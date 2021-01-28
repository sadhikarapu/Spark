from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

conf  = SparkConf() \
        .setAppName("TestApp")\
        .setMaster("local[3]")\

sc = SparkContext(conf=conf)

Spark = SparkSession(sc)

print(sc)
print(Spark)