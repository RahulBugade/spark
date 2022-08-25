from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="E:\\pySpark\\NOTS\\venu\\datasets\\donations1.csv"
# df=spark.read.format("csv").load(data)
rdd=spark.sparkContext.textFile(data)
skip=rdd.first()
odata=rdd.filter(lambda X:X!=skip)
df=spark.read.csv(odata,header=True)
df.printSchema()
df.show()