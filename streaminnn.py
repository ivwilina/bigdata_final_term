from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark = SparkSession.builder.getOrCreate()

input_csv_schema2 = spark.read.csv('D:/Coding Workspace/bigdata_final_term/New Folder/GoldPrice_2018.csv',header=True,inferSchema=True).schema

stream_df = spark \
                .readStream \
                    .schema(input_csv_schema2) \
                        .option('header','true') \
                            .format("csv") \
                                .option('path','D:/Coding Workspace/bigdata_final_term/New Folder/') \
                                    .load()


stream_df.printSchema()
tmin = stream_df.filter(col('Name').like("1/1/%"))
tmax = stream_df.filter(col('Name').like("12/31/%"))
tall = tmin.union(tmax)

tall2 = tall.agg(sum('US dollar1').alias('US')\
                ,sum('Euro18').alias('Eu')\
                ,sum('Japanese yen').alias('JP')\
                ,sum('Pound sterling').alias('Pou')\
                ,sum('Canadian dollar5').alias('Can')\
                ,sum('Swiss franc').alias('Sw')\
                ,sum('Indian rupee').alias('Indi')\
                ,sum('Chinese renmimbi8').alias('Chn')\
                ,sum('Turkish lira').alias('Tur')\
                ,sum('Saudi riyal').alias('Saudi')\
                ,sum('Indonesian rupiah').alias('Indo')\
                ,sum('UAE dirham').alias('UAE')\
                ,sum('Thai baht').alias('Thai')\
                ,sum('Vietnamese dong').alias('Vie')\
                ,sum('Egyptian pound').alias('Egypt')\
                ,sum('Korean won').alias('Kor')\
                ,sum('Russian ruble').alias('Rus')\
                ,sum('South African rand').alias('Afr')\
                ,sum('Australian dollar').alias('Aus')\
                )



activityQuery3 =  tall2.writeStream.format('console').outputMode('complete').start()


import time
time.sleep(60)
