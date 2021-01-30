from pyspark import SparkConf , SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().\
    setAppName("ReadFromHDFSStorage").\
    setMaster("local[*]")

sc=SparkContext(conf = conf)
spark = SparkSession(sc)

read_from_hdfs = spark.read.csv("hdfs:///user/asreekanth/Source_Files/AXISBANK__EQ__NSE__NSE__MINUTE.csv")

print(read_from_hdfs.show(10))