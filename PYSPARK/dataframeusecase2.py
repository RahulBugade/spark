from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="E:\\pySpark\\NOTS\\venu\\datasets\\bank-full.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","True").option("sep",";").load(data)
# res=df.select(col("age"),col("marital"), col("balance")).where((col("age")>60) & (col("marital")!="married"))
# res=df.where((col("age")>60) | (col("marital")=="married")&(col("balance")>=40000))
#res=df.where((col("age")>60) | (col("marital")=="married") & (col("balance")>=40000))
# res=df.groupBy(col("marital")).agg(sum(col("balance")).alias(""))
# res=df.where(col("age")>90)
# res=df.groupBy(col("marital")).count()
# res=df.groupBy(col("marital")).agg(sum(col("balance")).alias("smb")).orderBy(col("smb").desc())
df.createOrReplaceTempView("tab")
res=spark.sql("select*from tab where age >60 and balance <50000" )
res.show()

# res.printSchema()