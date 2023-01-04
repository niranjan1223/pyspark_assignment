# Databricks notebook source
# Spark_1 Assignment main code

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# create the session
def Session():
    spark = SparkSession.builder.master("local").appName("sample").getOrCreate()
    return spark

spark = Session()

# read the user csv file
def user(spark):
    user1 = spark.read.option('header', True).csv("/FileStore/tables/user__2_.csv", inferSchema=True)
    return user1

u=user(spark)
display(u)
u.printSchema()

# read the transcation csv file
def transaction(spark):
    trans = spark.read.option('header', True).csv("/FileStore/tables/transaction__2_.csv", inferSchema=True)
    return trans

t=transaction(spark)
display(t)
t.printSchema()

# join the files - inner join
def join_files(user1,trans):
    join = user1.join(trans)
    return join

j=join_files(u,t)
display(j)

# products bought by each user
def product_bought(j):
    return j.groupBy("userid","product_description").count()

p=product_bought(j)
display(p)

# Total spending done by each user on each product
def total_spending(j):
    return j.groupBy("userid","product_description").sum("price")

s=total_spending(j)
display(s)

# Count of unique locations where each product is sold
def uniq_loc(j):
    return j.groupBy("location ","product_description").count()\

l=uniq_loc(j)
display(l)

# COMMAND ----------

