from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="E:\\pySpark\\NOTS\\venu\\datasets\\10000Records.csv"
df=spark.read.format("csv").option("header","true").option("sep",",").option("inferschema","true").load(data)
# df.show(5,truncate=False)
import re
num=int(df.count())
cols=[re.sub('[^a-zA-z0-9]',"",c.lower())for c in df.columns]
ndf=df.toDF(*cols)
ndf.show(21,truncate=True)

# ndf.groupBy(col("gender")).agg((count(col("*")).alias("cnt"))
 # ndf.show()
# res.printSchema()