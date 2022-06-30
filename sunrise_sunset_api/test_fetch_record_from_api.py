from logging.handlers import TimedRotatingFileHandler
import logging
import os
from datetime import datetime
import configparser
import pytest
import ast
import pandas as pd
import requests
import boto3
from moto import mock_s3
from fetch_data_from_api_upload_s3 import (
    SunriseSunsetDataUploadS3,
    check_valid_date,
    checkvalid_lat,
    checkvalid_long,
    check_for_city_with_latitude_longitude
)
from fetch_data_partition_upload_local import ApiDataPartitionUploadLocal
from get_response_from_api import SunriseSunsetApi
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
    """this method returns the sunrise sunset api section from config object"""
    section = config_path["sunrise_sunset_api"]
    return section


@pytest.fixture
def logger_obj(parent_dir, config_path):
    """This method returns the logger object of time rotating time logger"""
    log_dir = os.path.join(
        parent_dir,
        config_path["local"]["log_path"],
        "sunrise_sunset_api_data_test",
    )
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_file = os.path.join(log_dir, "sunrise_sunset_test.log")
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
        parent_dir, config_path["local"]["data_path"], "sunrise_sunset_data_test"
    )
    if not os.path.exists(local_path):
        os.makedirs(local_path)
    return local_path


@pytest.fixture
def lat():
    """This method returns the latitude"""
    return 13.067439

@pytest.fixture
def city():
    """This method returns the city"""
    return "Chennai"

@pytest.fixture
def long():
    """This method returns the longitude"""
    return 80.237617

@pytest.fixture
def available_cities(config_section):
    """This method returns the available cities and its details in config"""
    cities=config_section["cities"]
    available_cities=ast.literal_eval(cities)
    return available_cities
@pytest.fixture
def new_city(city,lat,long):
    """This method creates the dict for the given lat, long, and city"""
    new_dict = {}
    new_dict[city] = {}
    new_dict[city]['lat'] = lat
    new_dict[city]['long'] =long
    return new_dict

@pytest.fixture
def startdate():
    """This method returns the startdate for getting the data"""
    startdate = datetime.strptime("2022-06-15", "%Y-%m-%d").date()
    return startdate


@pytest.fixture
def enddate():
    """This method returns the enddate for getting the data"""
    enddate = datetime.strptime("2022-06-20", "%Y-%m-%d").date()
    return enddate


@pytest.fixture
def date_none():
    """This method returns the startdate as none"""
    return None


@pytest.fixture
def false_endpoint():
    """This method returns the false endpoint"""
    endpoint = f"23947?sjhr?rehw:jher"
    return endpoint


@pytest.fixture
def endpoint(lat, long, startdate):
    """This method returns the endpoint for user packs"""
    endpoint = f"?lat={lat}&lng={long}&startdate={startdate}&formatted=0"
    return endpoint


@pytest.fixture
def partition_path(startdate,city):
    """This method returns the partition path based on id"""
    partition_path = startdate.strftime(
        f"pt_city={city}/pt_year=%Y/pt_month=%m/pt_day=%d"
    )
    return partition_path


@pytest.fixture
def file_name(startdate):
    """This method returns the file name"""
    return f"sunrise_sunset_on_{startdate}.json"


@pytest.fixture
def wrong_file_name(date_none):
    """This method returns the  wrong file name"""
    return f"data_on_{date_none}.json"


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
    base_url = "https://api.sunrise-sunset.org/json"
    return base_url


@pytest.fixture
def response(base_url, endpoint):
    """This method get the response from api for the given endpoint"""
    request_url = base_url + endpoint
    response = requests.get(request_url)
    if response.status_code == 200:
        response_json = response.json()
    else:
        response_json = None
    return response_json


@pytest.fixture
def false_response(base_url, false_endpoint):
    """This method get the false_response from api for the given endpoint"""
    try:
        request_url = base_url + false_endpoint
        response = requests.get(request_url)
        if response.status_code == 200:
            response_json = response.json()
        else:
            response_json = None
    except Exception as err:
        response_json = None
    return response_json


@pytest.fixture
def dataframe( response):
    """This method returns the dataframe from the response"""
    df_data = pd.DataFrame.from_dict(response["results"], orient="index")
    df_data = df_data.transpose()
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
        copy_source,
        wrong_file_name,
        partition_path,
    ):
        """This method will test for uploading json file to local file
        in class  ApiDataPartitionUploadLocal is done"""
        self.obj_local = ApiDataPartitionUploadLocal(logger_obj)
        dataframe.to_json(upload_path + "/" + wrong_file_name, orient="records", lines=True)
        local_file = self.obj_local.upload_partition_s3_local(
            upload_path, copy_source, wrong_file_name, partition_path
        )
        assert local_file is None


class TestSunriseSunsetApi:
    """This class has methods to test possible testcases in class SunriseSunsetApi"""

    def test_sunrise_sunset_api_object(self, config_section, logger_obj):
        """This method tests the instance for the sunrise sunset api"""
        self.api = SunriseSunsetApi(config_section, logger_obj)
        assert isinstance(self.api, SunriseSunsetApi)

    def test_get_endpoint_for_sunrise_sunset_done(
        self, config_section, logger_obj, lat, long, startdate
    ):
        """This method tests for getting endpoint for fetching sunrise and sunset datas from api"""
        self.api = SunriseSunsetApi(config_section, logger_obj)
        endpoint = self.api.get_endpoint_for_sunrise_sunset(startdate, lat, long)
        assert endpoint is not None

    @pytest.mark.xfail
    def test_get_endpoint_for_object_record_notdone(
        self, config_section, logger_obj, date_none, lat, long
    ):
        """This method tests for getting endpoint for fetching sunrise and sunset datas from api is failed"""
        self.api = SunriseSunsetApi(config_section, logger_obj)
        endpoint = self.api.get_endpoint_for_sunrise_sunset(date_none, lat, long)
        assert endpoint is None

    def test_get_response_from_api_done(self, config_section, logger_obj, endpoint):
        """This method tests for getting the response from api for the given endpoint is done"""
        self.api = SunriseSunsetApi(config_section, logger_obj)
        response = self.api.get_response_from_api(endpoint)
        assert response is not None

    @pytest.mark.xfail
    def test_get_response_from_api_notdone(self, config_section, logger_obj, false_endpoint):
        """This method tests for getting the response from api for the given endpoint is failed"""
        self.api = SunriseSunsetApi(config_section, logger_obj)
        response = self.api.get_response_from_api(false_endpoint)
        assert response is None


class TestSunriseSunsetDataUploadS3:
    """This class tests for possible testcases in SunriseSunsetDataUploadS3"""

    def test_fetch_record_s3_object(self, startdate, enddate, lat, long,city):
        """This method tests for instance of the class SunriseSunsetDataUploadS3"""
        self.obj = SunriseSunsetDataUploadS3(startdate, enddate, lat, long,city)
        assert isinstance(self.obj, SunriseSunsetDataUploadS3)

    def test_get_response_from_api_foreachcity_done(self, startdate, enddate, lat, long,city):
        """This method tests for getting response from sunrise sunset api for the given date, location is done"""
        self.obj = SunriseSunsetDataUploadS3(startdate, enddate, lat, long,city)
        response = self.obj.get_response_from_api(enddate, startdate,city,lat,long)
        assert response is not None

    @pytest.mark.xfail
    def test_get_response_from_api_foreachcity_notdone(self, date_none, startdate, enddate, lat, long,city):
        """This method tests for getting response from sunrise sunset api for the given date, location is notdone"""
        self.obj = SunriseSunsetDataUploadS3(startdate, enddate, lat, long,city)
        response = self.obj.get_response_from_api(startdate, date_none,city,lat,long)
        assert response is None

    def test_create_dataframe_for_response_done(
        self, response, startdate, enddate, lat, long,city
    ):
        """Thi smethod tests for creating dataframe for the response is done"""
        self.obj = SunriseSunsetDataUploadS3(startdate, enddate, lat, long,city)
        dataframe = self.obj.get_dataframe_for_response(response, startdate,city)
        assert isinstance(dataframe, pd.DataFrame)

    @pytest.mark.xfail
    def test_create_dataframe_for_response_notdone(
        self, false_response, startdate, enddate, lat, long,city
    ):
        """This method tests for creating dataframe for the response is notdone"""
        self.obj = SunriseSunsetDataUploadS3(startdate, enddate, lat, long,city)
        dataframe = self.obj.get_dataframe_for_response(false_response,None,city)
        assert dataframe is None

    def test_create_json_file_done(
        self, dataframe, file_name, partition_path, startdate, enddate, lat, long,city
    ):
        """This method tests for creating the json file for the dataframe is done"""
        self.obj = SunriseSunsetDataUploadS3(startdate, enddate, lat, long,city)
        file = self.obj.create_json_file_partition(dataframe, file_name, partition_path, startdate)
        assert file == file_name

    @pytest.mark.xfail
    def test_create_json_file_notdone(
        self, file_name, partition_path, startdate, enddate, lat, long,city
    ):
        """This method tests for creating the json file for the dataframe is not done"""
        self.obj = SunriseSunsetDataUploadS3(startdate, enddate, lat, long,city)
        dataframe = None
        file = self.obj.create_json_file_partition(dataframe, file_name, partition_path, startdate)
        assert file is None

    def test_get_lat_long_city_from_config_done(self, startdate, enddate, lat, long,city):
        """This method tests to get latitude , longitude and city values from config if user is not given is done"""
        self.obj = SunriseSunsetDataUploadS3(startdate, enddate, lat, long,city)
        response=self.obj.get_lat_long_city_from_config(enddate,startdate)
        assert response is not None
        
    @pytest.mark.xfail
    def test_get_lat_long_city_from_config_notdone(self, startdate, enddate,date_none, lat, long,city):
        """This method tests to get latitude , longitude and city values from config if user is not given is notdone"""
        self.obj = SunriseSunsetDataUploadS3(startdate, enddate, lat, long,city)
        response=self.obj.get_lat_long_city_from_config(startdate,date_none)
        assert response is  None
        
    def test_put_partition_path_done(self, partition_path, startdate, enddate, lat, long,city):
        """This method tests to put the partition path based on object id is done"""
        self.obj = SunriseSunsetDataUploadS3(startdate, enddate, lat, long,city)
        path = self.obj.put_partition_path(startdate,city)
        assert path == partition_path

    @pytest.mark.xfail
    def test_put_partition_path_notdone(self, startdate, lat, long, date_none,city):
        """This method tests to put the partition path based on object id is notdone"""
        self.obj = SunriseSunsetDataUploadS3(startdate, date_none, lat, long,city)
        path = self.obj.put_partition_path(date_none,city)
        assert path is None
    
    def test_update_config_is_done(self,new_city,startdate, date_none, lat, long,city,available_cities):
        """This method tests the new city and its details are updated in config"""
        self.obj = SunriseSunsetDataUploadS3(startdate, date_none, lat, long,city)
        result=self.obj.update_config_if_not_exists(new_city,available_cities)
        assert result is "success"
    
    @pytest.mark.xfail
    def test_update_config_is_notdone(self,new_city,startdate, date_none, lat, long,city,available_cities):
        """This method tests the new city and its details are not updated in config"""
        self.obj = SunriseSunsetDataUploadS3(startdate, date_none, lat, long,city)
        result=self.obj.update_config_if_not_exists(str(new_city),available_cities)
        assert result is "failed"

    def test_check_valid_city_done(self,lat,long,city):
        """This method checks the given city matches with the city of given latitude and longitude"""
        get_city=check_for_city_with_latitude_longitude(lat, long,city)
        assert get_city ==city

    @pytest.mark.xfail
    def test_check_valid_city_notdone(self,lat,long,city):
        """This method checks the given city not matches with the city of given latitude and longitude"""
        get_city=check_for_city_with_latitude_longitude(lat, long,"Chenna")
        assert get_city is None


    def test_check_lat_long_is_valid(self, lat, long):
        """This method checks the given latitide and longitude values are in the range"""
        valid_lat = checkvalid_lat(lat)
        valid_long = checkvalid_long(long)
        assert valid_lat == lat
        assert valid_long == long

    @pytest.mark.xfail
    def test_check_lat_long_is_notvalid(self):
        """This method checks the given latitide and longitude values are not in the range"""
        valid_lat = checkvalid_lat(-125.34)
        valid_long = checkvalid_long(200.334)
        assert valid_lat is None
        assert valid_long is None

    def test_check_date_is_valid(self, startdate):
        """This method checks the given date is in a valid format"""
        startdate = datetime.strftime(startdate, "%Y-%m-%d")
        valid_date = check_valid_date(startdate)
        date = datetime.strptime(startdate, "%Y-%m-%d").date()
        assert valid_date == date

    @pytest.mark.xfail
    def test_check_date_is_notvalid(self):
        """This method checks the given date is not in a valid format"""
        valid_date = check_valid_date("10-06-2022")
        assert valid_date is None
