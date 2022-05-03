"""This module tests the methods for pulling metaweather data for rach city in
    USA and upload them in partitioned path of s3"""
import os
import logging
import configparser
from datetime import datetime
from time import time
import pytest
import pandas as pd
import requests
from metaweather_data_api_partition_s3 import PullMetaWeatherDataUploadS3
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


@pytest.fixture
def start_date():
    """This method returns the start_date"""
    return "2022-05-02"


@pytest.fixture
def end_date():
    """This method returns the end_date"""
    return "2022-05-03"


@pytest.fixture
def city():
    """This method returns the cityname"""
    return "albuquerque"


@pytest.fixture
def woeid_id():
    """This method returns the woeid of city"""
    return "2352824"


class TestMetaWeatherApi:
    """This Test Class will check for all the possible Test cases in
    metaweather details api partion s3 module"""

    def test_pull_metaweather_api_objects(self, start_date, end_date):
        """This tests for the instance belong to the class PullMetaWeatherDataUploads3"""
        self.obj_metaapi = PullMetaWeatherDataUploadS3(logger_obj, start_date, end_date)
        assert isinstance(self.obj_metaapi, PullMetaWeatherDataUploadS3)

    def test_metaweather_data_for_givendates_passed(self, start_date, end_date):
        """This methods tests the days between the given dates"""
        startdate = datetime.strptime(start_date, "%Y-%m-%d").date()
        enddate = datetime.strptime(end_date, "%Y-%m-%d").date()
        self.obj_metaapi = PullMetaWeatherDataUploadS3(logger_obj, startdate, enddate)
        date = self.obj_metaapi.get_metaweather_data_for_givendates(enddate, startdate)
        # assert date is not None
        assert not isinstance(date, datetime)

    @pytest.mark.xfail
    def test_metaweather_data_for_givendates_failed(self, start_date, end_date):
        """This methods tests failed case for days between the given dates"""
        startdate = datetime.strptime(start_date, "%Y-%m-%d").date()
        enddate = datetime.strptime(end_date, "%Y-%m-%d").date()

        self.obj_metaapi = PullMetaWeatherDataUploadS3(logger_obj, startdate, enddate)
        date = self.obj_metaapi.get_metaweather_data_for_givendates(startdate, enddate)
        assert isinstance(date, datetime)

    def test_filter_group_response_by_hour_isdone(self, start_date, end_date, city, woeid_id):
        """This method tests for filtering the response from api based on hours"""
        search_date = datetime.now().date().strftime("%Y/%m/%d")
        response = requests.get(
            "https://www.metaweather.com/api/location/{woeid}/{date}/".format(
                woeid=woeid_id, date=search_date
            )
        ).json()
        self.obj_metaapi = PullMetaWeatherDataUploadS3(logger_obj, start_date, end_date)
        result = self.obj_metaapi.filter_group_response_by_hour(response, city, search_date)
        assert result is True

    @pytest.mark.xfail
    def test_filter_group_response_by_hour_is_notdone(self, start_date, end_date, city, woeid_id):
        """This method tests failure case for filtering the response from api based on hours"""
        search_date = datetime.now().date().strftime("%Y/%m/%d")
        response = requests.get(
            "https://www.metaweather.com/api/location/{woeid}/{date}/".format(
                woeid=woeid_id, date=search_date
            )
        ).json()
        self.obj_metaapi = PullMetaWeatherDataUploadS3(logger_obj, start_date, end_date)
        result = self.obj_metaapi.filter_group_response_by_hour(response, city, search_date)
        assert result is False

    def test_put_partition_path_isdone(self, start_date, end_date, city):
        """This method tests for giving the partition path based on city,year,month,date.hour"""
        self.obj_metaapi = PullMetaWeatherDataUploadS3(logger_obj, start_date, end_date)
        response = self.obj_metaapi.put_partition_path(city, "2022-05-03T07")
        assert response == "pt_city=albuquerque/pt_year=2022/pt_month=05/pt_day=03/pt_hour=07/"

    @pytest.mark.xfail
    def test_put_partition_path_is_notdone(self, start_date, end_date, city):
        """This method tests failure case for giving the partition path
        based on city,year,month,date.hour"""
        self.obj_metaapi = PullMetaWeatherDataUploadS3(logger_obj, start_date, end_date)
        response = self.obj_metaapi.put_partition_path(city, "2022-05-03T07")
        assert response != "pt_city=albuquerque/pt_year=2022/pt_month=05/pt_day=03/pt_hour=07/"

    def test_exceution_last_date_isupdated(self, start_date, end_date):
        """This method tests for the last date of execution is updated in config file"""
        self.obj_metaapi = PullMetaWeatherDataUploadS3(logger_obj, start_date, end_date)
        last_date = self.obj_metaapi.last_date_of_execution("")
        # last_date=config.get('last_execution_of_api','last_run_date')
        print(last_date)
        assert last_date == datetime.now().date()

    @pytest.mark.xfail
    def test_exceution_last_date_is_notupdated(self, start_date, end_date):
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

    def test_pull_data_from_each_city_passed(self, start_date, woeid_id):
        """This method tests for pulling metaweather data from each city
        with their woeid and date"""
        self.obj_pullapi = PullDataFromMetaweatherApi(logger_obj)
        startdate = datetime.strptime(start_date, "%Y-%m-%d").date()
        search_date = startdate.strftime("%Y/%m/%d")
        api_response = self.obj_pullapi.get_weather_data_cities_using_woeid_from_api(
            woeid_id, search_date
        )
        assert api_response is not None

    @pytest.mark.xfail
    def test_pull_data_from_each_city_failed(self, start_date, woeid_id):
        """This method tests failure case for pulling metaweather data from
        each city with their woeid and date"""
        self.obj_pullapi = PullDataFromMetaweatherApi(logger_obj)
        startdate = datetime.strptime(start_date, "%Y-%m-%d").date()
        search_date = startdate.strftime("%Y/%m/%d")
        api_response = self.obj_pullapi.get_weather_data_cities_using_woeid_from_api(
            woeid_id, search_date
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
        assert res == "flow of"


class TestUploadPartitionLocal:
    """This class has methods to test possible testcases in
    metaweather parition upload local module"""

    def test_parition_local_object(self):
        """This method tests the instance of class MetaweatherPartitionlocal"""
        self.obj_local = MetaweatherPartitionLocal(logger_obj)
        assert isinstance(self.obj_local, MetaweatherPartitionLocal)

    def test_write_data_to_local_file_is_done(self, city):
        """This method will test for the data is written to local file
        in class Metaweather Partition local"""
        self.obj_local = MetaweatherPartitionLocal(logger_obj)
        file_name = f"metaweather_{city}_{int(time())}.json"
        partition_path = "pt_city=albuquerque/pt_year=2022/pt_month=05/pt_day=03/pt_hour=07/"
        new_df = pd.DataFrame()
        local_file = self.obj_local.upload_parition_s3_local(new_df, file_name, partition_path)
        assert local_file == "json file created"

    @pytest.mark.xfail
    def test_write_data_to_local_file_is_not_done(self):
        """This method will test for the data is not written
        to local file in class Metaweather PartitionLocal"""
        self.obj_local = MetaweatherPartitionLocal(logger_obj)
        file_name = f"metaweather_{city}_{int(time())}.json"
        partition_path = "pt_city=albuquerque/pt_year=2022/pt_month=05/pt_day=03/pt_hour=07/"
        new_df = pd.DataFrame()
        local_file = self.obj_local.upload_parition_s3_local(new_df, file_name, partition_path)
        assert local_file == "json file not created"
