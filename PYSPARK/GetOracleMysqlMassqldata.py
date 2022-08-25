from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
host="jdbc:mysql://sqlrahul.cbr7z3ipx57d.ap-south-1.rds.amazonaws.com:3306/pillu?useSSL=false"
df=spark.read.format("jdbc").option("url",host).option("user","admin").option("password","Rahul.705212")\
    .option("dbtable","emp").option("driver","com.mysql.jdbc.Driver").load()

# df.show()

#process data
res=df.na.fill(0,['comm','mgr']).withColumn("comm",col("comm").cast(IntegerType()))\
    .withColumn("hirdate",to_date(col("hiredate"),"yyyy-MM-dd"))\
    .withColumn("hiredate",date_format(col("hiredate"),"yyyy/MMM/dd"))

res.write.format("jdbc").option("url",host).option("user","admin").option("password","Rahul.705212")\
    .option("dbtable","emp").option("driver","com.mysql.jdbc.Driver").save()


res.show()
res.printSchema()