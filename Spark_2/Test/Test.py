# Databricks notebook source
# MAGIC %run ../core/update_sample

# COMMAND ----------

import unittest


class UtilityTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spark = createSparkSession()
        cls.spark = spark
        
    def createdffortesttorrent(self):
        torrent_df = create_Torrent_df(self.spark, "_c0", "LogLevel", "_c1", "timestamp", "_c2", "ghtorrent_details","dbfs:/FileStore/tables/ghtorrent_logs__1_.txt")
        count = torrent_df.count()
        self.assertGreatEqual(count, 0)
        
    def split_req_Col(self):
        torrent_df = create_Torrent_df(self.spark, "_c0", "LogLevel", "_c1", "timestamp", "_c2", "ghtorrent_details",
                                     "dbfs:/FileStore/tables/ghtorrent_logs__1_.txt")
        torrent_df_extract = split_req_Col(torrent_df)
        actual_output = torrent_df_extract.columns
        expected_output = ['LogLevel', 'timestamp', 'ghtorrent_client_id', 'downloader_id', 'repository_torrent', 'Request_status_ext', 'Request_status', 'request_url']
        self.assertEqual(actual_output,expected_output)
        
        def testLinesofcountnumber(self):
            torrent_df = (self.spark, "_c0", "LogLevel", "_c1", "timestamp", "_c2", "ghtorrent_details",
                                     "dbfs:/FileStore/tables/ghtorrent_logs__1_.txt")
        no_of_lines = torrent_df.count()
        self.assertEqual(no_of_lines, 54)
        
        def test_WARN_count(self):
                torrent_df = create_Torrent_df(self.spark, "_c0", "LogLevel", "_c1", "timestamp", "_c2", "ghtorrent_details",
                                     "dbfs:/FileStore/tables/ghtorrent_logs__1_.txt")
        expected_count =  WARN_count(torrent_df, "LogLevel", "WARN").first()["warn_count"]
        self.assertEqual(expected_count, 3)
        
        def testreqApiClient(self):
                torrent_df = create_Torrent_df(self.spark, "_c0", "LogLevel", "_c1", "timestamp", "_c2", "ghtorrent_details",
                                     "dbfs:/FileStore/tables/ghtorrent_logs__1_.txt")
        torrent_df_extract = split_req_Col(torrent_df)
        expected_count = rep_api(torrent_df_extract, "api_client.rb", "repository_torrent").first()["api_client_repo_count"]
        self.assertEqual(expected_count,12)
        
        
        def ClientReq_http(self):
                torrent_df = create_Torrent_df(self.spark, "_c0", "LogLevel", "_c1", "timestamp", "_c2", "ghtorrent_details",
                                     "dbfs:/FileStore/tables/ghtorrent_logs__1_.txt")
        torrent_df_extract = split_req_Col(torrent_df)
        expected_ouput = ClientReq_http(torrent_df_extract, "request_url", "ghtorrent_client_id").first()["ghtorrent_client_id"]
        actual_output = " ghtorrent-5 "
        self.assertEqual(expected_ouput,actual_output)
        
        
        def testGetFailed_fail_http(self):
                torrent_df = create_Torrent_df(self.spark, "_c0", "LogLevel", "_c1", "timestamp", "_c2", "ghtorrent_details",
                                     "dbfs:/FileStore/tables/ghtorrent_logs__1_.txt")
        torrent_df_extract = split_req_col(torrent_df)
        expected_output = testGetFailed_fail_http(torrent_df_extract, "Request_status_ext", "%Failed%", "ghtorrent_client_id", "max_failed_req_client").first()["max_failed_req_client"]
        actual_output = " ghtorrent-13 "
        self.assertEqual(expected_output,actual_output)
        
        
        def test_A_hrs(self):
                torrent_df =  create_Torrent_df(self.spark, "_c0", "LogLevel", "_c1", "timestamp", "_c2", "ghtorrent_details",
                                     "dbfs:/FileStore/tables/ghtorrent_logs__1_.txt")
        torrent_df_extract = split_req_col(torrent_df)
        expected_output = test_A_hrs(torrent_df_extract).first()["active_hours"]
        actual_output = 16
        self.assertEqual(expected_output,actual_output)
        
        
        def test_active_repos(self):
                torrent_df = create_Torrent_df(self.spark, "_c0", "LogLevel", "_c1", "timestamp", "_c2", "ghtorrent_details",
                                    "dbfs:/FileStore/tables/ghtorrent_logs__1_.txt")
        torrent_df_extract = split_req_col(torrent_df)
        expected_output = countrepo(torrent_df_extract, "repository_torrent", "active_repo_used").first()["active_repo_used"]
        actual_output = "ghtorrent.rb"
        self.assertEqual(expected_output,actual_output)
        
        @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

# COMMAND ----------

