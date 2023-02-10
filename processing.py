#import
import pandas as pd
import glob
import json
import os
import shutil
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, length, col, expr, trim, sum
from pyspark.sql.types import IntegerType

#start timer
start_time = time.time()

# Create a Spark session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Artist Word Association") \
    .getOrCreate()

#read data
data1 = spark.read.json('data_split1.json', multiLine=True)
data2 = spark.read.json('data_split2.json', multiLine=True)
data3 = spark.read.json('data_split3.json', multiLine=True)
data4 = spark.read.json('data_split4.json', multiLine=True)

#select columns that we might need
deleteddf1 = data1.select('created_at', col("id").alias("tweet_id"), 'full_text', 'coordinates',
    'place','retweet_count', 'favorite_count','lang', 'entities.hashtags',
    'entities.symbols', 'entities.user_mentions', 'entities.urls', col('user.id').alias("userid"), 
    'user.name', 'user.screen_name', 'user.location', 'user.description', 'user.url', 
    'user.followers_count', 'user.profile_image_url_https', 'user.statuses_count')

deleteddf2 = data2.select('created_at', col("id").alias("tweet_id"), 'full_text', 'coordinates',
    'place','retweet_count', 'favorite_count','lang', 'entities.hashtags',
    'entities.symbols', 'entities.user_mentions', 'entities.urls', col('user.id').alias("userid"), 
    'user.name', 'user.screen_name', 'user.location', 'user.description', 'user.url', 
    'user.followers_count', 'user.profile_image_url_https', 'user.statuses_count')

deleteddf3 = data3.select('created_at', col("id").alias("tweet_id"), 'full_text', 'coordinates',
    'place','retweet_count', 'favorite_count','lang', 'entities.hashtags',
    'entities.symbols', 'entities.user_mentions', 'entities.urls', col('user.id').alias("userid"), 
    'user.name', 'user.screen_name', 'user.location', 'user.description', 'user.url', 
    'user.followers_count', 'user.profile_image_url_https', 'user.statuses_count')

deleteddf4 = data4.select('created_at', col("id").alias("tweet_id"), 'full_text', 'coordinates',
    'place','retweet_count', 'favorite_count','lang', 'entities.hashtags',
    'entities.symbols', 'entities.user_mentions', 'entities.urls', col('user.id').alias("userid"), 
    'user.name', 'user.screen_name', 'user.location', 'user.description', 'user.url', 
    'user.followers_count', 'user.profile_image_url_https', 'user.statuses_count')

#rename columns
deleteddf1 = deleteddf1.withColumn("created_at_month", substring("created_at", 5, 3)) \
    .withColumn("created_at_year", substring("created_at", 27, 4))
deleteddf1 = deleteddf1.drop('created_at')

deleteddf2 = deleteddf2.withColumn("created_at_month", substring("created_at", 5, 3)) \
    .withColumn("created_at_year", substring("created_at", 27, 4))
deleteddf2 = deleteddf2.drop('created_at')

deleteddf3 = deleteddf3.withColumn("created_at_month", substring("created_at", 5, 3)) \
    .withColumn("created_at_year", substring("created_at", 27, 4))
deleteddf3 = deleteddf3.drop('created_at')

deleteddf4 = deleteddf4.withColumn("created_at_month", substring("created_at", 5, 3)) \
    .withColumn("created_at_year", substring("created_at", 27, 4))
deleteddf4 = deleteddf4.drop('created_at')

#select, group, aggregate
info_df1 = deleteddf1.select('userid', 'screen_name', 'followers_count', 'statuses_count', 
    'tweet_id', 'created_at_month', 'created_at_year', 'retweet_count', 'favorite_count', 
    (deleteddf1['retweet_count'] + deleteddf1['favorite_count']).alias('sum_count')).   \
    groupBy('userid', 'screen_name' , 'created_at_year', 'created_at_month', 'followers_count', 'statuses_count'). \
    agg(sum('sum_count').alias('total_count'))
engage_df1 = info_df1.select('userid', 'screen_name', 'created_at_year', 'created_at_month', 
    'total_count', ((info_df1['total_count'] /info_df1['statuses_count']/info_df1['followers_count']) * 100).alias('engagement'))

info_df2 = deleteddf2.select('userid', 'screen_name', 'followers_count', 'statuses_count', 
    'tweet_id', 'created_at_month', 'created_at_year', 'retweet_count', 'favorite_count', 
    (deleteddf2['retweet_count'] + deleteddf2['favorite_count']).alias('sum_count')).   \
    groupBy('userid', 'screen_name' , 'created_at_year', 'created_at_month', 'followers_count', 'statuses_count'). \
    agg(sum('sum_count').alias('total_count'))
engage_df2 = info_df2.select('userid', 'screen_name', 'created_at_year', 'created_at_month', 
    'total_count', ((info_df2['total_count'] /info_df2['statuses_count']/info_df2['followers_count']) * 100).alias('engagement'))

info_df3 = deleteddf3.select('userid', 'screen_name', 'followers_count', 'statuses_count', 
    'tweet_id', 'created_at_month', 'created_at_year', 'retweet_count', 'favorite_count', 
    (deleteddf3['retweet_count'] + deleteddf3['favorite_count']).alias('sum_count')).   \
    groupBy('userid', 'screen_name' , 'created_at_year', 'created_at_month', 'followers_count', 'statuses_count'). \
    agg(sum('sum_count').alias('total_count'))
engage_df3 = info_df3.select('userid', 'screen_name', 'created_at_year', 'created_at_month', 
    'total_count', ((info_df3['total_count'] /info_df3['statuses_count']/info_df3['followers_count']) * 100).alias('engagement'))

info_df4 = deleteddf4.select('userid', 'screen_name', 'followers_count', 'statuses_count', 
    'tweet_id', 'created_at_month', 'created_at_year', 'retweet_count', 'favorite_count', 
    (deleteddf4['retweet_count'] + deleteddf4['favorite_count']).alias('sum_count')).   \
    groupBy('userid', 'screen_name' , 'created_at_year', 'created_at_month', 'followers_count', 'statuses_count'). \
    agg(sum('sum_count').alias('total_count'))
engage_df4 = info_df4.select('userid', 'screen_name', 'created_at_year', 'created_at_month', 
    'total_count', ((info_df4['total_count'] /info_df4['statuses_count']/info_df4['followers_count']) * 100).alias('engagement'))

#artistInfo dataframes
artistInfo_df1 = deleteddf1.select(col('name').alias('screenName'), col('screen_name').alias('handle'), col('profile_image_url_https').alias('URL')).distinct()
artistInfo_df2 = deleteddf2.select(col('name').alias('screenName'), col('screen_name').alias('handle'), col('profile_image_url_https').alias('URL')).distinct()
artistInfo_df3 = deleteddf3.select(col('name').alias('screenName'), col('screen_name').alias('handle'), col('profile_image_url_https').alias('URL')).distinct()
artistInfo_df4 = deleteddf4.select(col('name').alias('screenName'), col('screen_name').alias('handle'), col('profile_image_url_https').alias('URL')).distinct()

#merge dataframes
merge_df = engage_df1.union(engage_df2).union(engage_df3).union(engage_df4)

#output to csv
merge_df \
    .repartition(1) \
    .write.csv("output", sep="\t", header=True, mode="overwrite")

infomerge_df = artistInfo_df1.union(artistInfo_df2).union(artistInfo_df3).union(artistInfo_df4)

infomerge_df \
    .repartition(1) \
    .write.csv("infooutput", sep="\t", header=True, mode="overwrite")

#stop spark session and timer
spark.stop()
end_time = time.time()

print("Total running time: " + str(end_time - start_time) )