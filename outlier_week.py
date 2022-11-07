from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import sqlite3
import pandas as pd

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("snowflake-pyspark") \
    .getOrCreate()

df = spark.read.option("multiline","true").json("votes.json")
df.createOrReplaceTempView("outlier_week")
df2 = spark.sql("select * from outlier_week")

newdf = df2.withColumn("year",year(df2.CreationDate)).withColumn('week_number',weekofyear(df2.CreationDate))

newdf.createOrReplaceTempView("votes_per_week")
votesdf = spark.sql("select distinct CreationDate, week_number, year, count(Id) as votes from votes_per_week group by CreationDate, week_number, year order by CreationDate")
votesdf.show()

weekrecords = votesdf.groupby(col('year'), col('week_number')).agg(sum(col('votes')).alias('votes'), (avg(col('votes'))*0.1).alias('%')).orderBy('year','week_number')
weekrecords.show()

partition = Window.orderBy("year")
condition = lag(col("%"),1).over(partition) <= 20

outlier_df = weekrecords.withColumn("outlier", when(col('%')>20,'This week is outlier').when(condition,'This week is not outlier').otherwise('This week is not outlier'))
#pandas_df = outlier_df.toPandas()
#print(pandas_df)
outlier_df = outlier_df.select(col("year"), col("week_number"), col("votes"), col("outlier"))

pandas_df = outlier_df.toPandas()
#print(pandas_df)

conn = sqlite3.connect('pyspark_votes.db')
pandas_df.to_sql(name="votes_per_week", con=conn, if_exists="replace", index=False)
conn.commit()





'''
create_sql = "CREATE TABLE IF NOT EXISTS votes_per_week (year INTEGER, week_number INTEGER, votes INTEGER, outlier TEXT)"
query = con.cursor()
query.execute(create_sql)

for row in pandas_df.itertuples():
    insert_sql = f"INSERT INTO votes_per_week (year, week_number, votes, outlier) VALUES ({row[1]}, {row[2]}, {row[3]}, '{row[4]}')"
    query.execute(insert_sql)
'''


