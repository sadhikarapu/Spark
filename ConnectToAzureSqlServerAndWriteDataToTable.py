from pyspark import SparkContext , SparkConf
from pyspark.sql import *
from pyspark.sql.functions import col,lit,to_timestamp,from_utc_timestamp,to_date
from pyspark.sql.types import DateType,IntegerType,FloatType
import os

appname ='ExampleOfConnectingToAzureSQL'
master ="local"

conf = SparkConf() \
       .setAppName(appname) \
       .setMaster(master) \
       .set("spark.driver.extraClassPath","sqljdbc_8.4/enu/mssql-jdbc-8.4.1.jre8.jar")

sc = SparkContext(conf = conf).getOrCreate()
Spark = SparkSession(sc)

import_file  = Spark.read.csv("SourceFiles/registered_companies.csv",header=True,sep=',',inferSchema=True)
import_file = import_file.withColumn("DATE_OF_REGISTRATION", to_date(col("DATE_OF_REGISTRATION"), "dd-MM-yyyy"))

server = 'learnsparkazure.database.windows.net'
database = 'testdb'
username = 'sqladmin'
password = 'Pullman@99163'

jdbcUrl = f"jdbc:sqlserver://"+server+":1433;databaseName="+database+""
print(jdbcUrl)
jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

import_file.cache()

if __name__ == "__main__":

    Required_Years = ['2016','2017','2018','2019','2020']
    import_file.createOrReplaceTempView("Registered_Companies")

    for year in Required_Years:
        Data_file = Spark.sql("""
            Select 
             Registered_State
            ,year(Date_Of_Registration) as Year
            ,count(Distinct CORPORATE_IDENTIFICATION_NUMBER) as Number_Of_Companies_registered
            ,Row_Number() Over(Order By count(Distinct CORPORATE_IDENTIFICATION_NUMBER) desc )  as Yearly_Rank
            From Registered_Companies 
            Where Year(Date_Of_Registration)= %s
            Group By Registered_State,year(Date_Of_Registration)
            Order By 2 desc
             """% year)

        Data_file.write.format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("driver",jdbcDriver) \
            .option("user",username) \
            .option("password",password) \
            .option("dbtable", "[dbo].[Registered_Companies_Rank_ByYear]" ) \
            .save()

        print(Data_file)
print("Processing Completed")
