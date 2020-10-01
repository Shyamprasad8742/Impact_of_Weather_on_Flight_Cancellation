from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
sc.setLogLevel("ERROR")

#Loading data
df = spark.read.format("csv").option("header","true").load("/user/s2289741/*.csv")
df_airports = spark.read.format("csv").option("header","true").load("/user/s2331942/airport/airports.csv")
dfairport_a = df_airports.select("iata","city")
df_carriers = spark.read.format("csv").option("header","true").load("/user/s2331942/carrier/carriers.csv")
total_records = df.count()

#year wise split
df2004 = df[(df.Year == 2004)]
records_2004 = df2004.count()
df2005 = df[(df.Year == 2005)]
records_2005 = df2005.count()
df2006 = df[(df.Year == 2006)]
records_2006 = df2006.count()
df2007 = df[(df.Year == 2007)]
records_2007 = df2007.count()
df2008 = df[(df.Year == 2008)]
records_2008 = df2008.count()

#cancelled flights : year wise
df2004a = df2004[(df2004.Cancelled == 1)]
cancel_2004 = df2004a.count()
df2005a = df2005[(df2005.Cancelled == 1)]
cancel_2005 = df2005a.count()
df2006a = df2006[(df2006.Cancelled == 1)]
cancel_2006 = df2006a.count()
df2007a = df2007[(df2007.Cancelled == 1)]
cancel_2007 = df2007a.count()
df2008a = df2008[(df2008.Cancelled == 1)]
cancel_2008 = df2008a.count()

#Cancelled due to weather : year wise
df2004b = df2004a[(df2004a.CancellationCode == "B")]
weather_2004 = df2004b.count()
df2005b = df2005a[(df2005a.CancellationCode == "B")]
weather_2005 = df2005b.count()
df2006b = df2006a[(df2006a.CancellationCode == "B")]
weather_2006 = df2006b.count()
df2007b = df2007a[(df2007a.CancellationCode == "B")]
weather_2007 = df2007b.count()
df2008b = df2008a[(df2008a.CancellationCode == "B")]
weather_2008 = df2008b.count()

#Cancelled due to weather : year wise %
weather_perc_2004 = (float(weather_2004)/float(cancel_2004))*100
weather_perc_2005 = (float(weather_2005)/float(cancel_2005))*100
weather_perc_2006 = (float(weather_2006)/float(cancel_2006))*100
weather_perc_2007 = (float(weather_2007)/float(cancel_2007))*100
weather_perc_2008 = (float(weather_2008)/float(cancel_2008))*100

#Cancelled due to weather : months of each year
df2004c = df2004b.select(df2004b.Month)
r2004 = df2004c.rdd
r2004a = r2004 \
       .map(lambda x:(x,1)) \
       .reduceByKey(lambda a,b:a+b)
month_2004 = r2004a.top(12, key=lambda t: t[1])

df2005c = df2005b.select(df2005b.Month)
r2005 = df2005c.rdd
r2005a = r2005 \
       .map(lambda x:(x,1)) \
       .reduceByKey(lambda a,b:a+b)
month_2005 = r2005a.top(12, key=lambda t: t[1])

df2006c = df2006b.select(df2006b.Month)
r2006 = df2006c.rdd
r2006a = r2006 \
       .map(lambda x:(x,1)) \
       .reduceByKey(lambda a,b:a+b)
month_2006 = r2006a.top(12, key=lambda t: t[1])

df2007c = df2007b.select(df2007b.Month)
r2007 = df2007c.rdd
r2007a = r2007 \
       .map(lambda x:(x,1)) \
       .reduceByKey(lambda a,b:a+b)
month_2007 = r2007a.top(12, key=lambda t: t[1])

df2008c = df2008b.select(df2008b.Month)
r2008 = df2008c.rdd
r2008a = r2008 \
       .map(lambda x:(x,1)) \
       .reduceByKey(lambda a,b:a+b)
month_2008 = r2008a.top(4, key=lambda t: t[1])

#Cancelled due to weather : overall
dfwc = df[(df.Cancelled == 1) & (df.CancellationCode == "B")]
flights_cancelled_weather = dfwc.count()

#Cancelled due to weather : months overall
dfmonth = dfwc.select(df.Month)
rall = dfmonth.rdd
ralla = rall \
      .map(lambda x:(x,1)) \
      .reduceByKey(lambda a,b:a+b)
month_all = ralla.top(12, key=lambda t: t[1])
for (w,c) in month_all:
  print w,c

#Most affected routes year wise : top 5 highest route and counts of the least route
dfRoute2004 = df2004b.select(df2004b.Origin,df2004b.Dest)
dfRoute2004a = dfRoute2004.groupby(['Origin','Dest']).agg(f.count("*").alias('count'))  
dfRoute2004_final_desc = dfRoute2004a.orderBy('count',ascending=False)
dfR2004 = dfRoute2004_final_desc.join(dfairport_a, dfRoute2004_final_desc.Origin == dfairport_a.iata).drop('iata').withColumnRenamed("city","Origin City")
dfR2004a = dfR2004.join(dfairport_a, dfR2004.Dest == dfairport_a.iata).drop('iata').withColumnRenamed("city","Destination city")
dfRoute2004_final_asc = dfRoute2004a.orderBy('count',ascending=True)
dfRoute2004_final_asc.filter(f.col("count") == "1").count()
dfR2004a.show(5)

dfRoute2005 = df2005b.select(df2005b.Origin,df2005b.Dest)
dfRoute2005a = dfRoute2005.groupby(['Origin','Dest']).agg(f.count("*").alias('count'))  
dfRoute2005_final_desc = dfRoute2005a.orderBy('count',ascending=False)
dfR2005 = dfRoute2005_final_desc.join(dfairport_a, dfRoute2005_final_desc.Origin == dfairport_a.iata).drop('iata').withColumnRenamed("city","Origin City")
dfR2005a = dfR2005.join(dfairport_a, dfR2005.Dest == dfairport_a.iata).drop('iata').withColumnRenamed("city","Destination city")
dfRoute2005_final_asc = dfRoute2005a.orderBy('count',ascending=True)
dfRoute2005_final_asc.filter(f.col("count") == "1").count()
dfR2005a.show(5)

dfRoute2006 = df2006b.select(df2006b.Origin,df2006b.Dest)
dfRoute2006a = dfRoute2006.groupby(['Origin','Dest']).agg(f.count("*").alias('count'))  
dfRoute2006_final_desc = dfRoute2006a.orderBy('count',ascending=False)
dfR2006 = dfRoute2006_final_desc.join(dfairport_a, dfRoute2006_final_desc.Origin == dfairport_a.iata).drop('iata').withColumnRenamed("city","Origin City")
dfR2006a = dfR2006.join(dfairport_a, dfR2006.Dest == dfairport_a.iata).drop('iata').withColumnRenamed("city","Destination city")
dfRoute2006_final_asc = dfRoute2006a.orderBy('count',ascending=True)
dfRoute2006_final_asc.filter(f.col("count") == "1").count()
dfR2006a.show(5)

dfRoute2007 = df2007b.select(df2007b.Origin,df2007b.Dest)
dfRoute2007a = dfRoute2007.groupby(['Origin','Dest']).agg(f.count("*").alias('count'))  
dfRoute2007_final_desc = dfRoute2007a.orderBy('count',ascending=False)
dfR2007 = dfRoute2007_final_desc.join(dfairport_a, dfRoute2007_final_desc.Origin == dfairport_a.iata).drop('iata').withColumnRenamed("city","Origin City")
dfR2007a = dfR2007.join(dfairport_a, dfR2007.Dest == dfairport_a.iata).drop('iata').withColumnRenamed("city","Destination city")
dfRoute2007_final_asc = dfRoute2007a.orderBy('count',ascending=True)
dfRoute2007_final_asc.filter(f.col("count") == "1").count()
dfR2007a.show(5)

dfRoute2008 = df2008b.select(df2008b.Origin,df2008b.Dest)
dfRoute2008a = dfRoute2008.groupby(['Origin','Dest']).agg(f.count("*").alias('count'))  
dfRoute2008_final_desc = dfRoute2008a.orderBy('count',ascending=False)
dfR2008 = dfRoute2008_final_desc.join(dfairport_a, dfRoute2008_final_desc.Origin == dfairport_a.iata).drop('iata').withColumnRenamed("city","Origin City")
dfR2008a = dfR2008.join(dfairport_a, dfR2008.Dest == dfairport_a.iata).drop('iata').withColumnRenamed("city","Destination city")
dfRoute2008_final_asc = dfRoute2008a.orderBy('count',ascending=True)
dfRoute2008_final_asc.filter(f.col("count") == "1").count()
dfR2008a.show(5)

#Most affected airlines year wise : top 5 affected airlines and the least affected one

dfAirway2004 = df2004b.select(df2004b.UniqueCarrier)
dfAirway2004a = dfAirway2004.groupby(['UniqueCarrier']).agg(f.count("*").alias('count'))  
dfAirway2004_final_desc = dfAirway2004a.orderBy('count',ascending=False)
dfA2004_desc = dfAirway2004_final_desc.join(df_carriers, dfAirway2004_final_desc.UniqueCarrier == df_carriers.Code).drop('Code').withColumnRenamed("Description","Airways")
dfAirway2004_final_asc = dfAirway2004a.orderBy('count',ascending=True)
dfA2004_asc = dfAirway2004_final_asc.join(df_carriers, dfAirway2004_final_asc.UniqueCarrier == df_carriers.Code).drop('Code').withColumnRenamed("Description","Airways")
dfA2004_final_asc = dfA2004_asc.show(1)
dfA2004_final_desc = dfA2004_desc.show(5)

dfAirway2005 = df2005b.select(df2005b.UniqueCarrier)
dfAirway2005a = dfAirway2005.groupby(['UniqueCarrier']).agg(f.count("*").alias('count'))  
dfAirway2005_final_desc = dfAirway2005a.orderBy('count',ascending=False)
dfA2005_desc = dfAirway2005_final_desc.join(df_carriers, dfAirway2005_final_desc.UniqueCarrier == df_carriers.Code).drop('Code').withColumnRenamed("Description","Airways")
dfAirway2005_final_asc = dfAirway2005a.orderBy('count',ascending=True)
dfA2005_asc = dfAirway2005_final_asc.join(df_carriers, dfAirway2005_final_asc.UniqueCarrier == df_carriers.Code).drop('Code').withColumnRenamed("Description","Airways")
dfA2005_final_asc = dfA2005_asc.show(1)
dfA2005_final_desc = dfA2005_desc.show(5)


dfAirway2006 = df2006b.select(df2006b.UniqueCarrier)
dfAirway2006a = dfAirway2006.groupby(['UniqueCarrier']).agg(f.count("*").alias('count'))  
dfAirway2006_final_desc = dfAirway2006a.orderBy('count',ascending=False)
dfA2006_desc = dfAirway2006_final_desc.join(df_carriers, dfAirway2006_final_desc.UniqueCarrier == df_carriers.Code).drop('Code').withColumnRenamed("Description","Airways")
dfAirway2006_final_asc = dfAirway2006a.orderBy('count',ascending=True)
dfA2006_asc = dfAirway2006_final_asc.join(df_carriers, dfAirway2006_final_asc.UniqueCarrier == df_carriers.Code).drop('Code').withColumnRenamed("Description","Airways")
dfA2006_final_asc = dfA2006_asc.show(1)
dfA2006_final_desc = dfA2006_desc.show(5)

dfAirway2007 = df2007b.select(df2007b.UniqueCarrier)
dfAirway2007a = dfAirway2007.groupby(['UniqueCarrier']).agg(f.count("*").alias('count'))  
dfAirway2007_final_desc = dfAirway2007a.orderBy('count',ascending=False)
dfA2007_desc = dfAirway2007_final_desc.join(df_carriers, dfAirway2007_final_desc.UniqueCarrier == df_carriers.Code).drop('Code').withColumnRenamed("Description","Airways")
dfAirway2007_final_asc = dfAirway2007a.orderBy('count',ascending=True)
dfA2007_asc = dfAirway2007_final_asc.join(df_carriers, dfAirway2007_final_asc.UniqueCarrier == df_carriers.Code).drop('Code').withColumnRenamed("Description","Airways")
dfA2007_final_asc = dfA2007_asc.show(1)
dfA2007_final_desc = dfA2007_desc.show(5)

dfAirway2008 = df2008b.select(df2008b.UniqueCarrier)
dfAirway2008a = dfAirway2008.groupby(['UniqueCarrier']).agg(f.count("*").alias('count'))  
dfAirway2008_final_desc = dfAirway2008a.orderBy('count',ascending=False)
dfA2008_desc = dfAirway2008_final_desc.join(df_carriers, dfAirway2008_final_desc.UniqueCarrier == df_carriers.Code).drop('Code').withColumnRenamed("Description","Airways")
dfAirway2008_final_asc = dfAirway2008a.orderBy('count',ascending=True)
dfA2008_asc = dfAirway2008_final_asc.join(df_carriers, dfAirway2008_final_asc.UniqueCarrier == df_carriers.Code).drop('Code').withColumnRenamed("Description","Airways")
dfA2008_final_asc = dfA2008_asc.show(1)
dfA2008_final_desc = dfA2008_desc.show(5)
