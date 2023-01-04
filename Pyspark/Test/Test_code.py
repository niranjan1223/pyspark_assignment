# Databricks notebook source
# MAGIC %run ../core/Sample

# COMMAND ----------

# pyspark test code
from pyspark.sql import *
import unittest



class PysparkUnittest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spark=SparkSession.builder.master('local').appName('test_code').getOrCreate()
        cls.spark=spark

    def test_table1(self):
        sample = table_1(self.spark)
        result = sample.count()
        self.assertEqual(result,3)

    def test_table2(self):
        sample = table_2(self.spark)
        result = sample.count()
        self.assertEqual(result,3)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()  # stop the spark session

# COMMAND ----------

