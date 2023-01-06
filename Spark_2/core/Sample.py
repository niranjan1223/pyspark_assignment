# Databricks notebook source
# spark_2 Assignment
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import split, col, count, hour, minute, second

spark=SparkSession.builder.appName("sparkbyexamples").getOrCreate()
header_schems = StructType([
    StructField("Log", StringType(), True),
    StructField("date-time-stamp", StringType(), True),
    StructField("user_id", StringType(), True),StructField("torrent", StringType(), True)])

csvFiles = spark.read.option("header",True).schema(header_schems).csv("/FileStore/tables")


df1 = csvFiles.withColumn('torrent',split(col("user_id"),"--").getItem(0)) \
            .withColumn('getapiclient',split(col("user_id"),"--").getItem(1)) \
            .withColumn('clientresult',split(col("getapiclient"),":").getItem(0)) \
            .withColumn('req_client',split(col("getapiclient"),":").getItem(1)) \
            .withColumn('request', split(col("req_client"), ".").getItem(0))

df1.show(truncate=False)

count_line = df1.agg(count("*").alias("total_count"))
count_line.sort("total_count").show()

wranlog = df1.filter("Log = 'WARN'").agg(count("*").alias("totalwran_count"))
wranlog.sort("totalwran_count").show()

rep_api = df1.filter(col("clientresult").like("%api_client.rb%")).agg(count("*").alias("api_claim"))
rep_api.sort("api_claim").show()

http_req = df1.groupBy("torrent").agg(count("*").alias("http_req_count"))
http_req.sort("http_req_count").show()

Failed_req = df1.filter(col("req_client").like("%Failed%")).groupBy("torrent").agg(count("*").alias("Failed_request"))
Failed_req.sort("Failed_request").show()

active_hour = df1.withColumn("hour", hour(col("date-time-stamp"))).groupBy("hour").agg(count("*").alias("active_hours"))
active_hour.sort("active_hours").show(truncate=False)

active_repository = df1.groupBy("torrent").agg(count("*").alias("active_repository"))
active_repository.sort("active_repository").show(truncate=False)

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import split, col, count, hour, minute, second


def createSession():
    spark=SparkSession.builder.appName("sparkbyexamples").getOrCreate()
    return spark
s=createSession()


def create_Torrent_df(s,col1,rename_col_1,col2,rename_col_2,col3,rename_col_3, path):
    df = s.read.csv(path='dbfs:/FileStore/tables/ghtorrent_logs__1_.txt')
    torrent_df = df.withColumnRenamed(col1,rename_col_1).withColumnRenamed(col2,rename_col_2).withColumnRenamed(col3,rename_col_3)
    return torrent_df
df1 = create_Torrent_df(s, "_c0", "LogLevel", "_c1", "timestamp", "_c2",
                              "ghtorrent_details","../../resource/ghtorrent-logs.txt")


def split_req_Col(ghtorrent_log_df):
    api_cliendId= ghtorrent_log_df .withColumn("ghtorrent_client_id", split(col("ghtorrent_details"), "--").getItem(0)) \
        .withColumn("repository", split(col("ghtorrent_details"), "--").getItem(1)) \
        .withColumn("downloader_id", split(col("ghtorrent_client_id"), "-").getItem(1)) \
        .withColumn("repository_torrent", split(col("repository"), ':').getItem(0)) \
        .withColumn("Request_status_ext", split(col("repository"), ':').getItem(1)) \
        .withColumn("Request_status", split(col("Request_status_ext"), ",").getItem(0)) \
        .drop(col("repository")) \
        .withColumn("request_url", split(col("ghtorrent_details"), "URL:").getItem(1)) \
        .drop(col("ghtorrent_details"))
    return api_cliendId

ghtorrent_df = split_req_Col(df1)
print("Printing the torrent df")
display(ghtorrent_df)

#  How many lines does gh_torrent contain
def count_lines(df1):
    count_line = df1.agg(count("*").alias("total_count"))
    count_line.sort("total_count")
    return count_line

total_count = count_lines(ghtorrent_df)
print("Total number of lines")
display(total_count)

# count the number of WARNing messages
def WARN_count(df1):
    wranlog = df1.filter("LogLevel = 'WARN'").agg(count("*").alias("totalwran_count"))
    wranlog.sort("totalwran_count")
    return wranlog
num_WARN = WARN_count(df1)
print("number of WARNing")
display(num_WARN)

# How many repositories where processed in total? Use the api_client lines only
def rep_api(df1):
    rep_api = df1.filter(col("ghtorrent_details").like("%api_client.rb%")).agg(count("*").alias("api_claim"))
    rep_api.sort("api_claim")
    return rep_api
api=rep_api(df1)
print("api_cilent repositories")
display(api)


# which client did most HTTP requests?
def http_req(df1):
    http_req = df1.groupBy("ghtorrent_details").agg(count("*").alias("http_req_count"))
    http_req.sort("http_req_count")
    return http_req
http = http_req(df1)
print("active http requests")
display(http)

# Which client did most FAILED HTTP requests? Use group by to provide an answer
def fail_http(df1):
    Failed_req = df1.filter(col("ghtorrent_details").like("%Failed%")).groupBy("ghtorrent_details").agg(count("*").alias("Failed_request"))
    Failed_req.sort("Failed_request")
    return Failed_req
fail_https=fail_http(df1)
print("FAILED HTTP requests")
display(fail_https)

# What is the most active hour of day
def active_hr(df1):
    active_hr = df1.withColumn("hour", hour(col("timestamp"))).groupBy("hour").agg(count("*").alias("active_hours"))
    active_hr.sort("active_hours")
    return active_hr
ac_hr = active_hr(df1)
print("most active hour of day")
display(ac_hr)

# count of Active repos
def active_repos(df1):
    active_repository = df1.groupBy("ghtorrent_details").agg(count("*").alias("active_repository"))
    active_repository.sort("active_repository")
    return active_repository
ac_re = active_repos(df1)
print("count of Active repos")
display(ac_re)

# COMMAND ----------


