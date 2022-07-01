from logging.handlers import TimedRotatingFileHandler
import logging
import os
from datetime import datetime
import configparser
from numpy import isin
import pytest
import ast
import pandas as pd
import requests
import boto3
from moto import mock_s3
from fetch_data_from_api_upload_s3 import PublicHolidaysLongWeekendS3, valid_year, valid_region
from fetch_data_partition_upload_local import ApiDataPartitionUploadLocal
from get_response_from_holidayapi import HolidayApi
from aws_s3.s3_details import S3Details


@pytest.fixture
def parent_dir():
    """this method returns the parent directory"""
    parent_dir = os.path.dirname(os.getcwd())
    return parent_dir


@pytest.fixture
def config_path(parent_dir):
    """this method returns the config object"""
    config = configparser.ConfigParser()
    config.read(parent_dir + "/details.ini")
    return config


@pytest.fixture
def config_section(config_path):
    """this method returns the holiday api section from config object"""
    section = config_path["holiday_api"]
    return section


@pytest.fixture
def logger_obj(parent_dir, config_path):
    """This method returns the logger object of time rotating time logger"""
    log_dir = os.path.join(
        parent_dir,
        config_path["local"]["log_path"],
        "holiday_api_data_test",
    )
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_file = os.path.join(log_dir, "holiday_api_test.log")
    logging.basicConfig(
        level=logging.INFO,
        filename=log_file,
        datefmt="%d-%b-%y %H:%M:%S",
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
        filemode="a",
    )
    logger_obj = logging.getLogger("Rotating Log")
    handler = TimedRotatingFileHandler(log_file, when="h", interval=1, backupCount=3)
    logger_obj.addHandler(handler)
    return logger_obj


@pytest.fixture
def upload_path(parent_dir, config_path):
    """This method returns the sql path"""
    local_path = os.path.join(
        parent_dir, config_path["local"]["data_path"], "holiday_api_data_test"
    )
    if not os.path.exists(local_path):
        os.makedirs(local_path)
    return local_path


@pytest.fixture
def start_year():
    """This method returns the startyear for which datas are needed"""
    return 2020


@pytest.fixture
def end_year():
    """This method returns the endyear for which datas are needed"""
    return 2022


@pytest.fixture
def unavailable_year():
    """This method returns the unavailable start_year"""
    return "1900"


@pytest.fixture
def region():
    """This method returns the region for which datas are needed"""
    return "MX"


@pytest.fixture
def regions_list():
    """This method returns the list of regions"""
    return ['MX']

@pytest.fixture
def false_region():
    """This method returns the false_region for which datas are needed"""
    return "IN"


@pytest.fixture
def epoint():
    """This method returns the endpoint name for datas to be fetched"""
    return "PublicHolidays"


@pytest.fixture
def false_endpoint(start_year, epoint, false_region):
    """This method returns the false endpoint"""
    endpoint = f"{epoint}/{start_year}/{false_region}"
    return endpoint


@pytest.fixture
def endpoint_ph_lw(start_year, epoint, region):
    """This method returns the endpoint for public holidays and long weekend"""
    endpoint = f"{epoint}/{start_year}/{region}"
    return endpoint


@pytest.fixture
def endpoint_nextph(region):
    """This method returns the endpoint for next public holidays"""
    endpoint = f"NextPublicHolidays/{region}"
    return endpoint


@pytest.fixture
def partition_path(region, start_year):
    """This method returns the partition path based on region and start_year"""
    partition_path = f"pt_region={region}/pt_year={start_year}"
    return partition_path


@pytest.fixture
def wrong_partition_path(false_region, start_year):
    """This method returns the wrong partition path based on region and start_year"""
    partition_path = f"pt_region={false_region}/pt_year={start_year}"
    return partition_path


@pytest.fixture
def file_name(epoint, start_year):
    """This method returns the file name"""
    file_name = f"{epoint}_on_{start_year}.json"
    return file_name


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def s3_mock(aws_credentials):
    """This method creates a mock for s3 services"""
    with mock_s3():
        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture
def key(partition_path, file_name):
    key_name = partition_path + file_name
    return key_name


@pytest.fixture
def file(parent_dir, file_name, upload_path):
    file = os.path.join(parent_dir, upload_path, file_name)
    return file


@pytest.fixture
def no_file():
    return None


@pytest.fixture
def bucket_name(config_section):
    bucket_name = config_section["bucket"]
    return bucket_name


@pytest.fixture
def bucket_path(config_section):
    bucket_path = config_section["s3_path"]
    return bucket_path


@pytest.fixture
def s3_mock_client():
    """This is the fixture for mocking s3 service"""
    with mock_s3():
        conn = boto3.client("s3", region_name="us-east-1")
        yield conn


@pytest.fixture
def s3_bucket(s3_mock_client, bucket_name):
    """This is the fixture for creating s3 bucket"""
    res = s3_mock_client.create_bucket(Bucket=bucket_name)
    yield res


@pytest.fixture
def s3_upload(s3_mock_client, bucket_name, s3_bucket, file, bucket_path):
    """This fixture is to upload file in s3 mock bucket"""
    upload = s3_mock_client.put_object(Bucket=bucket_name, Body=file, Key=bucket_path)
    yield


@pytest.fixture
def base_url():
    """This method returns the base url of the api"""
    base_url = "https://date.nager.at/api/v3/"
    return base_url


@pytest.fixture
def response(base_url, endpoint_ph_lw):
    """This method get the response from api for the given endpoint"""
    request_url = base_url + endpoint_ph_lw
    response = requests.get(request_url)
    if response.status_code == 200:
        response_json = response.json()
    else:
        response_json = None
    return response_json


@pytest.fixture
def false_response(base_url, false_endpoint):
    """This method get the false_response from api for the given endpoint"""
    request_url = base_url + false_endpoint
    response = requests.get(request_url)
    if response.status_code == 200:
        response_json = response.json()
    else:
        response_json = None
    return response_json


@pytest.fixture
def dataframe(response):
    """This method returns the dataframe from the response"""
    df_data = pd.DataFrame(response)
    return df_data


@pytest.fixture
def copy_source(upload_path, file_name):
    """This method creates and returns the temporary copy source"""
    return upload_path + "/" + file_name


class TestS3:
    """This class will test all possible testcases in s3 module"""

    def test_s3_object(self, logger_obj, config_path):
        """This method test the instance belong to the class of S3Service"""
        self.s3_obj = S3Details(logger_obj, config_path)
        assert isinstance(self.s3_obj, S3Details)

    def test_upload_file_s3_done(
        self,
        file,
        config_path,
        key,
        bucket_name,
        s3_mock_client,
        s3_bucket,
        bucket_path,
        logger_obj,
    ):
        """This method will test file is sucessfully uploaded"""
        self.my_client = S3Details(logger_obj, config_path)
        file = self.my_client.upload_file(file, bucket_name, bucket_path, key)
        assert file is not None

    @pytest.mark.xfail
    def test_upload_file_S3_notdone(
        self,
        config_path,
        key,
        no_file,
        bucket_name,
        s3_mock_client,
        s3_bucket,
        bucket_path,
        logger_obj,
    ):
        """This method will test file is not uploaded"""
        self.my_client = S3Details(logger_obj, config_path)
        file = self.my_client.upload_file(no_file, bucket_name, bucket_path, key)
        assert file is None


class TestUploadLocal:
    """This class tests the possible testcases in ApiDataPartitionUploadLocal for
    uploading files in local s3 path"""

    def test_parition_local_object(self, logger_obj):
        """This method tests the instance of class ApiDataPartitionUploadLocal"""
        self.obj_local = ApiDataPartitionUploadLocal(logger_obj)
        assert isinstance(self.obj_local, ApiDataPartitionUploadLocal)

    def test_upload_file_to_local_path_is_done(
        self,
        logger_obj,
        dataframe,
        file_name,
        upload_path,
        copy_source,
        partition_path,
    ):
        """This method will test for uploading json file to local file
        in class  ApiDataPartitionUploadLocal is done"""
        self.obj_local = ApiDataPartitionUploadLocal(logger_obj)
        dataframe.to_json(upload_path + "/" + file_name, orient="records", lines=True)
        local_file = self.obj_local.upload_partition_s3_local(
            upload_path, copy_source, file_name, partition_path
        )
        assert local_file == file_name

    @pytest.mark.xfail
    def test_upload_file_to_local_path_is_not_done(
        self,
        logger_obj,
        dataframe,
        upload_path,
        file_name,
        partition_path,
    ):
        """This method will test for uploading json file to local file
        in class  ApiDataPartitionUploadLocal is done"""
        self.obj_local = ApiDataPartitionUploadLocal(logger_obj)
        dataframe.to_json(upload_path + "/" + file_name, orient="records", lines=True)
        local_file = self.obj_local.upload_partition_s3_local(
            upload_path, None, file_name, partition_path
        )
        assert local_file is None


class TestHolidayApi:
    """This class has methods to test possible testcases in Holidayapi"""

    def test_holiday_api_object(self, config_section, logger_obj):
        """This method tests for the instances of the class HolidayApi"""
        self.api = HolidayApi(config_section, logger_obj)
        assert isinstance(self.api, HolidayApi)

    def test_check_availability_of_year_done(self, config_section, logger_obj, start_year):
        """This method checks the given start_year is available in the limit of the holiday api"""
        self.api = HolidayApi(config_section, logger_obj)
        available_year = self.api.check_availability_of_year(start_year)
        assert available_year == start_year

    @pytest.mark.xfail
    def test_check_availability_of_year_notdone(self, config_section, logger_obj, unavailable_year):
        """This method checks the given start_year is available in the limit of the holiday api"""
        self.api = HolidayApi(config_section, logger_obj)
        available_year = self.api.check_availability_of_year(unavailable_year)
        assert available_year is None

    def test_get_endpoint_publiholidays_done(
        self, config_section, logger_obj, start_year, region, epoint
    ):
        """This method tests to get endpoint for the publi holidays"""
        self.api = HolidayApi(config_section, logger_obj)
        response = self.api.get_endpoint_for_public_holidays_long_weekend(
            start_year, region, epoint
        )
        assert response is not None

    @pytest.mark.xfail
    def test_get_endpoint_publiholidays_notdone(
        self, config_section, logger_obj, unavailable_year, region, epoint
    ):
        """This method tests to get endpoint for the publi holidays"""
        self.api = HolidayApi(config_section, logger_obj)
        response = self.api.get_endpoint_for_public_holidays_long_weekend(
            unavailable_year, region, epoint
        )
        assert response is None

    def test_get_response_ph_lw_from_apidone(self, config_section, logger_obj, endpoint_ph_lw):
        """This method tests for getting the response for public holidays or long weekend from api for the
        given start_year, region and endpoint"""
        self.api = HolidayApi(config_section, logger_obj)
        response = self.api.get_response_from_api(endpoint_ph_lw)
        assert response is not None

    @pytest.mark.xfail
    def test_get_response_ph_lw_from_api_notdone(self, config_section, logger_obj, false_endpoint):
        """This method tests for getting the response for public holidays or long weekend from api for the
        given start_year, region and endpoint"""
        self.api = HolidayApi(config_section, logger_obj)
        response = self.api.get_response_from_api(false_endpoint)
        assert response is None

    def test_get_response_nextph_from_apidone(self, config_section, logger_obj, endpoint_nextph):
        """This method tests for getting the response for next public holidays from api for the given start_year,
        region and endpoint"""
        self.api = HolidayApi(config_section, logger_obj)
        response = self.api.get_response_from_api(endpoint_nextph)
        assert response is not None

    @pytest.mark.xfail
    def test_get_response_nextph_from_api_notdone(self, config_section, logger_obj, false_endpoint):
        """This method tests for getting the response  for next public holidays from api for the given start_year,
        region and endpoint"""
        self.api = HolidayApi(config_section, logger_obj)
        response = self.api.get_response_from_api(false_endpoint)
        assert response is None


class TestFetchFromApiUploadS3:
    """This class has methods to test possible testcases in PublicHolidayLongWeekendS3"""

    def test_fetch_api_upload_s3_object(self, start_year, end_year, region):
        self.obj = PublicHolidaysLongWeekendS3(start_year, end_year, region)
        assert isinstance(self.obj, PublicHolidaysLongWeekendS3)

    def test_get_years_list_isdone(self, start_year, end_year, region):
        """This method tests for getting the list of years between the given start and end year"""
        self.obj = PublicHolidaysLongWeekendS3(start_year, end_year, region)
        years_list = self.obj.get_year_list_from_given_years()
        assert isinstance(years_list, list)

    def test_get_response_from_api_done(self, start_year, end_year, region, epoint,regions_list):
        """This method tests fro getting the resposne from api for the given endpoint"""
        self.obj = PublicHolidaysLongWeekendS3(start_year, end_year, region)
        years_list = self.obj.get_year_list_from_given_years()
        response = self.obj.get_response_from_api(years_list, epoint,regions_list)
        assert response is not None

    @pytest.mark.xfail
    def test_get_response_from_api_notdone(self, start_year, end_year, region, false_endpoint,regions_list):
        """This method tests fro getting the resposne from api for the given endpoint"""
        self.obj = PublicHolidaysLongWeekendS3(start_year, end_year, region)
        years_list = self.obj.get_year_list_from_given_years()
        response = self.obj.get_response_from_api(years_list, false_endpoint,regions_list)
        assert response is None

    def test_create_dataframe_for_response_done(
        self, start_year, end_year, region, file_name, partition_path, epoint, response
    ):
        """This method tests for creating the dataframe for the given response is done"""
        self.obj = PublicHolidaysLongWeekendS3(start_year, end_year, region)
        df_data = self.obj.get_dataframe_for_response(
            response, start_year, file_name, partition_path, epoint
        )
        assert isinstance(df_data, pd.DataFrame)

    @pytest.mark.xfail
    def test_create_dataframe_for_response_notdone(
        self, start_year, end_year, region, file_name, partition_path, epoint, false_response
    ):
        """This method tests for creating the dataframe for the given response is not done"""
        self.obj = PublicHolidaysLongWeekendS3(start_year, end_year, region)
        df_data = self.obj.get_dataframe_for_response(
            false_response, start_year, file_name, partition_path, epoint
        )
        assert df_data is None

    def test_create_json_file_done(
        self, start_year, end_year, region, file_name, partition_path, epoint, dataframe
    ):
        """This method tests for creating the json file from the dataframe is done"""
        self.obj = PublicHolidaysLongWeekendS3(start_year, end_year, region)
        file = self.obj.create_json_file_partition(
            dataframe, start_year, file_name, partition_path, epoint
        )
        assert file is file_name

    @pytest.mark.xfail
    def test_create_json_file_notdone(
        self, start_year, end_year, region, file_name, partition_path, epoint, dataframe
    ):
        """This method tests for creating the json file from the dataframe is not done"""
        self.obj = PublicHolidaysLongWeekendS3(start_year, end_year, region)
        file = self.obj.create_json_file_partition(
            None, start_year, file_name, partition_path, epoint
        )
        assert file is None

    def test_get_endpoint_region_lists_done(self,start_year, end_year, regions_list,epoint):
        """This method tests for getting the endpoints and their corresponding region lists"""
        self.obj = PublicHolidaysLongWeekendS3(start_year, end_year, regions_list)
        response=self.obj.get_endpoint_region_lists(epoint)
        assert response is not None
        
    @pytest.mark.xfail
    def test_get_endpoint_region_lists_notdone(self,start_year, end_year, regions_list,false_endpoint):
        """This method tests for getting the endpoints and their corresponding region lists is failed"""
        self.obj = PublicHolidaysLongWeekendS3(start_year,None, regions_list)
        response=self.obj.get_endpoint_region_lists(false_endpoint)
        assert response is None

    def test_check_region_is_available_done(self,start_year, end_year, regions_list,config_section):
        """This method checks the new region is not available in config"""
        self.obj = PublicHolidaysLongWeekendS3(start_year, end_year, regions_list)
        available_regions=config_section["publicholiday_region_list"]
        region_name="publicholiday_region_list"
        regions=self.obj.check_region_is_available(available_regions, region_name)
        assert regions is not None
    
    @pytest.mark.xfail
    def test_check_region_is_available_notdone(self,start_year, end_year, region,config_section):
        """This method checks the new region is available in config"""
        self.obj = PublicHolidaysLongWeekendS3(start_year, end_year, region)
        available_regions=config_section["publicholiday_region_list"]
        region_name="publicholiday_region_list"
        regions=self.obj.check_region_is_available(available_regions, region_name)
        assert regions == []

    def test_put_partition_path_isdone(self, start_year, end_year, region, epoint, partition_path):
        """This method tests the partition path is given based on the year, region"""
        self.obj = PublicHolidaysLongWeekendS3(start_year, end_year, region)
        path = self.obj.put_partition_path(start_year, epoint,region)
        assert path == partition_path

    @pytest.mark.xfail
    def test_put_partition_path_notdone(
        self, start_year, end_year, false_region, epoint, partition_path, wrong_partition_path
    ):
        """This method tests the partition path is given based on the year, region"""
        self.obj = PublicHolidaysLongWeekendS3(start_year, end_year, false_region)
        path = self.obj.put_partition_path(start_year, epoint,false_region)
        assert path == wrong_partition_path
        
    def test_update_config_is_done(self,start_year, end_year, false_region,regions_list,config_section):
        """This method tests the new regions are updated in config"""
        self.obj = PublicHolidaysLongWeekendS3(start_year, end_year, false_region)
        available_regions=ast.literal_eval(config_section["publicholiday_region_list"])
        region_name="publicholiday_region_list"
        result=self.obj.update_config_if_not_exists(region_name, available_regions,regions_list)
        assert result is "Updated"
    
    @pytest.mark.xfail
    def test_update_config_is_notdone(self,start_year, end_year, false_region,regions_list,config_section):
        """This method tests the new regions are not updated in config"""
        self.obj = PublicHolidaysLongWeekendS3(start_year, end_year, false_region)
        available_regions=ast.literal_eval(config_section["publicholiday_region_list"])
        region_name="publicholiday_region_list"
        result=self.obj.update_config_if_not_exists(region_name, available_regions,str(regions_list))
        assert result is "failed"
        
    def test_year_is_valid(self, start_year):
        """This method tests the given year is valid"""
        valid_years = valid_year(str(start_year))
        assert isinstance(valid_years, int)

    @pytest.mark.xfail
    def test_not_valid_year(self):
        """This method tests the given year is not a valid"""
        valid_years = valid_year("jhsjjd")
        import argparse

        assert valid_years is None

    def test_region_is_valid(self, region):
        """This method tests the given region is available in holiday api"""
        available_reg = valid_region(region)
        assert available_reg == region

    @pytest.mark.xfail
    def test_region_is_notvalid(self, false_region):
        """This method tests the given region is not available in holiday api"""
        available_reg = valid_region(false_region)
        assert available_reg is None
