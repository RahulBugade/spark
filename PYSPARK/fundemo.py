from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="E:\\pySpark\\NOTS\\venu\\datasets\\us-500.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
ndf=df.groupBy(df.state).agg(count("*").alias("cnt")).orderBy(col("cnt").desc())
ndf.show()
df.show()