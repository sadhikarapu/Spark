from pyspark import SparkConf , SparkContext
from pyspark.sql import SparkSession

# let us create the conf for creating an app regarding reading from database

conf = SparkConf() \
       .setAppName("ReadFromAzureSqlDataBase") \
       .setMaster("local[3]") \
       .set("spark.driver.extraClassPath","sqljdbc_8.4/enu/mssql-jdbc-8.4.1.jre8.jar")

sc = SparkContext(conf= conf).getOrCreate()

# let us create a sparksession named Spark

Spark = SparkSession(sc)

# read from database table [dbo].[Registered_Companies_Rank_ByYear]

servername ="learnsparkazure.database.windows.net"
username="sqladmin"
password="Pullman@99163"
databasename="testdb"

jdbcurl= f"jdbc:sqlserver://"+servername+":1433;databaseName="+databasename
jdbcdriver="com.microsoft.sqlserver.jdbc.SQLServerDriver"

read_data_registeredcompanies = Spark.read.format("jdbc") \
                                .option("url",jdbcurl) \
                                .option("driver",jdbcdriver) \
                                .option("user",username) \
                                .option("password",password) \
                                .option("dbtable","[dbo].[Registered_Companies_Rank_ByYear]") \
                                .load()

if __name__ == "__main__":
    read_data_registeredcompanies.createOrReplaceTempView("Registered_Company_Analysis")
    print(Spark.sql("Select * From Registered_Company_Analysis Where Registered_State='Telangana' Order By Year").show(10))