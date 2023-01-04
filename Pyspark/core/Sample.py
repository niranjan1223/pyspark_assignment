# Databricks notebook source
# Pyspark Assignment
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

def Session():
    spark = SparkSession.builder.appName('ass1').getOrCreate()
    return spark
Session()

#reading data from csv
def Read_file1(spark):
    df = spark.read.option("header",True).option("inferSchema",True).csv("/FileStore/tables/Table1.csv")
    return df

table_1=Read_file1(spark)
display(table_1)

#1.converting issue date to timestamp format

def unix_to_timestamp(table_1):
    df1 = table_1.withColumn("equal_time",from_unixtime(col("Issue Date")/1000))
    return df1
d1=unix_to_timestamp(table_1)
display(d1)

#2.converting timestamp to date format

def timestamp_to_date(d1):
    df2 = d1.withColumn("date type", to_date("equal_time"))
    return df2
d2=timestamp_to_date(d1)
display(d2)

#3.removing null values in country column

def removing_null(d2):
    df3 = d2.fillna(value="",subset='Country')
    return df3
rn = removing_null(d2)
display(rn)

#4.removing the space
def removing_space(rn):
    df4 = rn.withColumn("Brand",trim("Brand"))
    return df4
rs = removing_space(rn)
display(rs)



# create table 2
def Read_file2(spark):
    table2 = spark.read.option("header",True).csv('/FileStore/tables/Table2.csv',inferSchema=True)
    return table2

t2=Read_file2(spark)
display(t2)


# change columns heading from camelcase to snake case
def change_case(t2):
    Table2 = t2.withColumnRenamed('SourceId','source_id').withColumnRenamed('TransactionNumber','transaction_number').\
        withColumnRenamed('Language','language').withColumnRenamed('ModelNumber','model_number').\
        withColumnRenamed('StartTime','start_time').withColumnRenamed('ProductNumber','product_number')
    return Table2

T2 = change_case(t2)
display(T2)


# convert the values of StartTime to milliseconds.
def convert_time(T2):
    ct = T2.withColumn('time_ms',unix_timestamp(col('start_time')))
    return ct

st = convert_time(T2)
display(st)

# combine table 1 and table 2
def combine_tables(rs,st):
    join = rs.join(st,rs.product_number == st.product_number,"inner")
    return join

j=combine_tables(rs,st)
display(j)

# get the country as EN
def get_EN(j):
    final = j.where(j.Country == "EN")
    return final

EN=get_EN(j)
display(EN)



# COMMAND ----------

