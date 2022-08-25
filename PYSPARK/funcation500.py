from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="E:\\pySpark\\NOTS\\venu\\datasets\\us-500.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
# ndf=df.groupBy(df.city).agg(count("*").alias("cnt"),collect_set(df.first_name)).orderBy((col("cnt").desc()))
# ndf=df.withColumn("state",when(col("state")=="NY","NEW YORK").when(col("state")=="CA","Cali").otherwise(col("state")))
# ndf=df.withColumn("address1",when(col("address").contains("#"),"******").otherwise(col("address")))\
#     .withColumn("address2",regexp_replace(col("address"),"#","_"))

# ndf1=df.withColumn("substr",substring(col("email"),0,5)).withColumn("emails",substring_index(col("email"),"@",-1)).withColumn("username",substring_index(col("email"),"@",1))
# #ndf1=df.withColumn("substr",substring(col("email"),0,5)).withColumn("emails", substring_index(col("email"),"@",-1)).withColumn("username", substring_index(col("email"),"@",1))#
# ndf=ndf1.groupBy(col("emails")).count().orderBy(col("count").desc())
df.createTempView("tab")
qry="""with tmp as (select *,concat_ws('_',first_name,last_name)fullnanme,substring_index(email,'@',-1)mail from tab)
select mail,count(*) cnt from  tmp group by mail order by cnt desc
"""
# ndf=spark.sql(qry)
#create u r own funcation
def func(st):
    if(st=="NY"):
        return "30% off"
    elif(st=="CA"):
        return "40% off"
    elif(st=="OH"):
        return "50% off"
    else:
        return "500/- off"
uf=udf(func)
spark.udf.register("offer",uf)
ndf=spark.sql("select * ,offer(state) todayoffer from tab")
# ndf=df.withColumn("offer",uf(col("state")))

ndf.show(truncate=False)
ndf.printSchema()
