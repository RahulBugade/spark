from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("test").config("spark.sql.session.timeZone","EST").getOrCreate()
data="E:\\pySpark\\NOTS\\venu\\datasets\\donations.csv"
df=spark.read.format("csv").option("header","true").load(data)
res = df
# def daystoyrmndays(nums):
#     yrs = int(nums / 365)
#     mon = int((nums % 365) / 30)
#     days = int((nums % 365) % 30)
#     result = yrs, "years" , mon , "months" , days, "days"
#     st = ''.join(map(str, result))
#     return st
# .withColumn("daystoyrmon", udffunc(col("dtdiff")))
# udffunc = udf(daystoyrmndays)
res=df.withColumn("dt",to_date(col("dt"),"d-M-yyyy")).withColumn("today",current_date())\
    .withColumn("ts",current_timestamp()).withColumn("dtdiff",datediff(col("today"),col("dt")))\
    .withColumn("dtadd",date_add(col("dt"),-100))\
    .withColumn("dtsub",date_sub(col("dt"),100))\
    .withColumn("lastdt",last_day(col("dt")))\
    .withColumn("lastday",last_day(col("dt")))\
    .withColumn("next_day",next_day(col("today"),"Mon"))\
    .withColumn("dtformat",date_format(col("dt"),"dd/MM/yy"))\
    .withColumn("yr",month(col("dt")))\
    .withColumn("monbet",months_between(current_date(),col("dt")))\
    .withColumn("ceil",ceil(col("monbet")))\
    .withColumn("floor",floor(col("monbet")))\
    .withColumn("round",round(col("monbet")).cast(IntegerType()))\
    .withColumn("dttrunc",date_trunc("day",col("dt")).cast(DateType()))

res.printSchema()
res.show(3)