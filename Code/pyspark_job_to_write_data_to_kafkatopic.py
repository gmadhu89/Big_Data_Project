from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime
import time
from pyspark.sql.functions import split
from pyspark.sql.functions import *
from pyspark.sql import Window
sc = SparkContext(appName='dezyre_test')
sc.setLogLevel('WARN')
spark = SparkSession(sc)

#Read streaming data from Kafka into Pyspark dataframe
dfCSV=spark.readStream.format('kafka').option('kafka.bootstrap.servers','localhost:9092').option('subscribe', 'dezyre_corona_data')\
                      .option("failOnDataLoss","false").option('startingOffsets', 'earliest').load().selectExpr("CAST(value AS STRING)")
dfCSV.printSchema()

#Define schema for the data
userSchema =StructType([
StructField('Country_name',StringType()),
StructField('Country_code',StringType()),
StructField('Country_newconfirmed',StringType()),
StructField('Country_new_deaths',StringType()),
StructField('Country_new_recovered',StringType()),
StructField('Country_total_confirmed',StringType()),
StructField('Country_total_deaths',StringType()),
StructField('Country_total_recovered',StringType()),
StructField('Extracted_timestamp',StringType()),
StructField('Global_New_Confirmed',StringType()),
StructField('Global_New_Deaths',StringType()),
StructField('Global_New_Recovered',StringType()),
StructField('Global_Total_Confirmed',StringType()),
StructField('Global_Total_Deaths',StringType()),
StructField('Global_Total_Recovered',StringType())
])


#Parse the data
def parse_data_from_kafka_message(sdf, schema):
  from pyspark.sql.functions import split
  assert sdf.isStreaming == True, "DataFrame doesn't receive streaming data"
  col = split(sdf['value'], ',') #split attributes to nested array in one Column
  #now expand col to multiple top-level columns
  for idx, field in enumerate(schema):
      sdf  = sdf.withColumn(field.name, col.getItem(idx).cast(field.dataType))
  return sdf.select([field.name for field in schema])
dfCSV = parse_data_from_kafka_message(dfCSV, userSchema)


#Process the data
# Getting Percentage of Total Deaths / Recovered per country globally
#q_Old=dfCSV.groupBy("Country_code","Country_name","Country_total_deaths","Extracted_timestamp").count()

import pyspark.sql.functions as f
q = dfCSV.withColumn("Percent_Total_Deaths", (f.col('Country_total_deaths')/f.col('Global_Total_Deaths'))*100 ) \
                 .withColumn("Percent_Total_Recovered",(f.col('Country_total_recovered')/f.col('Global_Total_Recovered'))*100 ) \
                 .select("Country_code","Country_name","Country_total_deaths","Global_Total_Deaths","Country_total_recovered","Global_Total_Recovered",'Percent_Total_Deaths','Percent_Total_Recovered','Extracted_timestamp')


#Write streaming data to output Kafka topic which can be consumed by destination services like #HDFS, Nifi, etc.
q2=q.select(to_json(struct(
'Country_code',
'Country_name',
'Country_total_deaths','Extracted_timestamp','Percent_Total_Deaths','Percent_Total_Recovered')) \
.alias('value'))

final = q2.writeStream.format("kafka").outputMode("append") \
.option("failOnDataLoss","true").option('checkpointLocation','/home/ubuntu/checkpoint_out') \
.option("kafka.bootstrap.servers","localhost:9092") \
.option("topic", "dezyre_out").start().awaitTermination()



'''
##Below code is only for Reference ##
## OutpuMode : Append (Will only take new rows/ any changes that come in the streaming data)

# Command to display the data from Kafka in console format

query = dfCSV.writeStream.format("console").option("numRows",50).start()
import time
time.sleep(5)
query.stop()

q_Old.writeStream.format("console").option("numRows",50).start()

query1 = q2.writeStream.format("console").option("numRows",50).start()
## To view streaming data in the console
dfCSV.writeStream.format("console").outputMode("append").start().awaitTermination()


final = q2.writeStream.format("kafka").outputMode("append") \
.option("failOnDataLoss","false").option('checkpointLocation','/home/ubuntu/checkpoint_out') \
.option("kafka.bootstrap.servers","localhost:9092") \
.option("topic", "dezyre_out").start().awaitTermination()

final2 = q2.writeStream.format("csv").option("format","append") \
.option("path","/home/ubuntu/test.csv").option('checkpointLocation','/home/ubuntu/checkpoint_out') \
.outputMode("append") \
.start()

test=spark.readStream.format('kafka').option('kafka.bootstrap.servers','localhost:9092').option('subscribe', 'dezyre_out')\
                      .option("failOnDataLoss","false").option('startingOffsets', 'earliest').load().selectExpr("CAST(value AS STRING)")
query2 = test.writeStream.format("console").option("numRows",50).start()
'''

