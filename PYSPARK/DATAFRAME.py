import pyspark

from pyspark.sql import *
from pyspark.sql.functions import *

# creating SparkSession object
spark = SparkSession.builder.master("local[*]").appName("sparkdf").getOrCreate()
#
#data="C:\\BigData\\datasets-20220727T020123Z-001\\datasets\\donations.csv"
#df=spark.read.format("csv").option("header","true").load(data)
#if u mention header true, first line of data consider as columns
#if u have any mal records like first line second line wrong clean that data using rdd or udf.
#skip first line second line onwards original data available.
data="E:\\pySpark\\NOTS\\venu\\datasets\\donations.csv"
rdd=spark.sparkContext.textFile(data)
skip=rdd.first()
odata= rdd.filter(lambda x:x!=skip)
df=spark.read.csv(odata,header=True,inferSchema=True)
df.printSchema()

#printing columns and its datatype in nice tree format.
df.show(5)