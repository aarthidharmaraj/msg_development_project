"""This module tests the methods for pulling metaweather data for rach city in
    USA and upload them in partitioned path of s3"""
import os
import logging
import configparser
from datetime import datetime
import pytest
from metaweather_details_api_partition_s3 import PullMetaWeatherDataUploadS3
from metaweather_info_partition_local import MetaweatherPartitionLocal
from pull_data_each_city_from_api import PullDataFromMetaweatherApi
from aws_s3.s3_details import S3Details

parent_dir = os.path.dirname(os.getcwd())
config = configparser.ConfigParser()
config.read(parent_dir + "/details.ini")


logging.basicConfig(
    level=logging.INFO,
    filename="metaweather_logfiletest.log",
    datefmt="%d-%b-%y %H:%M:%S",
    format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    filemode="w",
)
logger_obj = logging.getLogger()
start_date = "2022-04-27"
end_date = "2022-04-29"


class TestMetaWeatherApi:
    """This Test Class will check for all the possible Test cases in 
        metaweather details api partion s3 module"""

    def test_pull_metaweather_api_objects(self):
        """This tests for the instance belong to the class PullMetaWeatherDataUploads3"""
        self.obj_metaapi = PullMetaWeatherDataUploadS3(logger_obj, start_date, end_date)
        assert isinstance(self.obj_metaapi, PullMetaWeatherDataUploadS3)

    def test_metaweather_data_for_givendates_passed(self):
        """This methods tests the days between the given dates"""
        startdate = datetime.strptime(start_date, "%Y-%m-%d").date()
        enddate = datetime.strptime(end_date, "%Y-%m-%d").date()
        self.obj_metaapi = PullMetaWeatherDataUploadS3(logger_obj, startdate, enddate)
        date = self.obj_metaapi.get_metaweather_data_for_givendates(enddate, startdate)
        # assert date is not None
        assert not isinstance(date, datetime)

    @pytest.mark.xfail
    def test_metaweather_data_for_givendates_failed(self):
        """This methods tests failed case for days between the given dates"""
        startdate = datetime.strptime(start_date, "%Y-%m-%d").date()
        enddate = datetime.strptime(end_date, "%Y-%m-%d").date()

        self.obj_metaapi = PullMetaWeatherDataUploadS3(logger_obj, startdate, enddate)
        date = self.obj_metaapi.get_metaweather_data_for_givendates(startdate, enddate)
        assert isinstance(date, datetime)

    def test_exceution_last_date_isupdated(self):
        """This method tests for the last date of execution is updated in config file"""
        self.obj_metaapi = PullMetaWeatherDataUploadS3(logger_obj, start_date, end_date)
        last_date = self.obj_metaapi.last_date_of_execution("")
        # last_date=config.get('last_execution_of_api','last_run_date')
        print(last_date)
        assert last_date == datetime.now().date()

    @pytest.mark.xfail
    def test_exceution_last_date_is_notupdated(self):
        """This method tests for the last date of execution is updated in config file"""
        self.obj_metaapi = PullMetaWeatherDataUploadS3(logger_obj, start_date, end_date)
        self.obj_metaapi.last_date_of_execution()
        last_date = config.get("last_execution_of_api", "last_run_date")
        assert last_date != datetime.now().date()


class TestPullDataForEachCity:
    """This class tests for possible testcases in pull data from each city module"""

    def test_pull_data_city_object(self):
        """This tests for the instance belong to the class PullDataFromMetaweatherApi"""
        self.obj_pullapi = PullDataFromMetaweatherApi(logger_obj)
        assert isinstance(self.obj_pullapi, PullDataFromMetaweatherApi)

    def test_pull_data_from_each_city_passed(self):
        """ This method tests for pulling metaweather data from each city 
            with their woeid and date"""
        self.obj_pullapi = PullDataFromMetaweatherApi(logger_obj)
        woeid = "2352824"
        startdate = datetime.strptime(start_date, "%Y-%m-%d").date()
        search_date = startdate.strftime("%Y/%m/%d")
        api_response = self.obj_pullapi.get_weather_data_cities_using_woeid_from_api(
            woeid, search_date
        )
        assert api_response is not None

    @pytest.mark.xfail
    def test_pull_data_from_each_city_failed(self):
        """This method tests failure case for pulling metaweather data from
           each city with their woeid and date"""
        self.obj_pullapi = PullDataFromMetaweatherApi(logger_obj)
        woeid = "2352824"
        startdate = datetime.strptime(start_date, "%Y-%m-%d").date()
        search_date = startdate.strftime("%Y/%m/%d")
        api_response = self.obj_pullapi.get_weather_data_cities_using_woeid_from_api(
            woeid, search_date
        )
        assert api_response is None


class TestS3Details:
    """This Test Class will check for all the possible
        Test cases in S3Operations"""

    def test_s3operations_objects(self):
        """This Method will test for the instance belong to the class S3Operations"""
        self.obj = S3Details(logger_obj)
        assert isinstance(self.obj, S3Details)

    def test_upload_s3_passed(self):
        """This tests for put object in S3 class S3details"""
        self.s3_client = S3Details(logger_obj)
        res = self.s3_client.upload_file("flow of code", "flow.txt")
        assert res == "flow of code"

    @pytest.mark.xfail
    def test_upload_s3_failed(self):
        """This method will test for the report is uploaded to S3 in class S3Operations"""
        self.s3_client = S3Details(logger_obj)
        res = self.s3_client.upload_file(b"flow of code", "flow.txt")
        assert res != "flow of code"


class TestUploadPartitionLocal:
    """This class has methods to test possible testcases in
        metaweather parition upload local module"""
    def test_parition_local_object(self):
        """This method tests the instance of class MetaweatherPartitionlocal"""
        self.obj_local = MetaweatherPartitionLocal(logger_obj)
        assert isinstance(self.obj_local,MetaweatherPartitionLocal)

    def test_write_data_to_local_file_is_done(self):
        """ This method will test for the data is written to local file 
            in class Metaweather Partition local"""
        self.obj_local = MetaweatherPartitionLocal(logger_obj)
        path = "local/test/metaweather_api/"
        local_file = self.obj_local.upload_parition_s3_local(path, "weather_data")
        assert local_file == "json file created"


    @pytest.mark.xfail
    def test_write_data_to_local_file_is_not_done(self):
        """ This method will test for the data is not written
            to local file in class S3Operations """
        self.obj_local = MetaweatherPartitionLocal(logger_obj)
        path='local/test/metaweather_api/'
        local_file = self.obj_local.upload_parition_s3_local(path, 'weather_data')
        assert local_file=='json file not created'
