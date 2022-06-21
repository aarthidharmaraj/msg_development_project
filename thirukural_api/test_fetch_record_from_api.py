from logging.handlers import TimedRotatingFileHandler
import logging
import os
from datetime import datetime
import configparser
import pytest
import pandas as pd
import requests
import boto3
from moto import mock_s3
from fetch_data_from_api_upload_s3 import ThirukuralDataUploadS3,check_valid_kuralnumber
from fetch_data_partition_upload_local import ApiDataPartitionUploadLocal
from get_response_from_api import ThirukuralApi
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
    """this method returns the thirukural api section from config object"""
    section = config_path["thirukural_api"]
    return section


@pytest.fixture
def logger_obj(parent_dir, config_path):
    """This method returns the logger object of time rotating time logger"""
    log_dir = os.path.join(
        parent_dir,
        config_path["local"]["log_path"],
        "thirukural_api_test",
    )
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_file = os.path.join(log_dir, "thirukural_test.log")
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
        parent_dir, config_path["local"]["data_path"], "thirukural__data_test"
    )
    if not os.path.exists(local_path):
        os.makedirs(local_path)
    return local_path


@pytest.fixture
def kural_num():
    """This method returns the kural number for getting kural details"""
    return 34


@pytest.fixture
def invalid_num():
    """This method returns the invalid_num for getting kural details"""
    return 1332


@pytest.fixture
def false_endpoint(invalid_num):
    """This method returns the false endpoint"""
    endpoint = f"?num/{invalid_num}"
    return endpoint


@pytest.fixture
def endpoint(kural_num):
    """This method returns the endpoint for getting the kural_number"""
    endpoint = f"?num={kural_num}"
    return endpoint


@pytest.fixture
def partition_path(kural_num,response):
    """This method returns the partition path based on section, chapter and number"""
    sect = response["sect_eng"]
    chapgrp = response["chapgrp_eng"]
    chap = response["chap_eng"]
    partition_path =f"pt_section={sect}/pt_chaptergroup={chapgrp}/pt_chapter={chap}/pt_number={kural_num}"
    return partition_path


@pytest.fixture
def file_name(kural_num):
    """This method returns the file name"""
    return f"data_of_kural_{kural_num}.json"


@pytest.fixture
def wrong_file_name(invalid_num):
    """This method returns the  wrong file name"""
    return f"{invalid_num}.json"


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
    base_url = "https://api-thirukkural.vercel.app/api"
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
        response_json=None
    return response_json


@pytest.fixture
def dataframe(response):
    """This method returns the dataframe from the response"""
    df_data = pd.DataFrame.from_dict(response, orient="index")
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
        dataframe.to_json(upload_path + "/" + file_name, orient="records",force_ascii=False, lines=True)
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


class TestThirukuralApi:
    """This class has methods to test possible testcases in class ThirukuralApi"""

    def test_thirukural_api_object(self, config_section, logger_obj):
        """This method tests the instance for the thirukural api"""
        self.api = ThirukuralApi(config_section, logger_obj)
        assert isinstance(self.api, ThirukuralApi)

    def test_get_endpoint_for_object_record_done(self, config_section, logger_obj, kural_num):
        """This method tests for getting endpoint for fetching thirukural data from api"""
        self.api = ThirukuralApi(config_section, logger_obj)
        endpoint = self.api.get_endpoint_for_thirukural_data(kural_num)
        assert endpoint is not None

    @pytest.mark.xfail
    def test_get_endpoint_for_object_record_notdone(self, config_section, logger_obj, invalid_num):
        """This method tests for getting endpoint for fetching thirukural data from api is failed"""
        self.api = ThirukuralApi(config_section, logger_obj)
        endpoint = self.api.get_endpoint_for_thirukural_data(invalid_num)
        assert endpoint is None

    def test_get_response_from_api_done(self, config_section, logger_obj, endpoint):
        """This method tests for getting the response from api for the given endpoint is done"""
        self.api = ThirukuralApi(config_section, logger_obj)
        response = self.api.get_response_from_api(endpoint)
        assert response is not None

    @pytest.mark.xfail
    def test_get_response_from_api_notdone(self, config_section, logger_obj, false_endpoint):
        """This method tests for getting the response from api for the given endpoint is failed"""
        self.api = ThirukuralApi(config_section, logger_obj)
        response = self.api.get_response_from_api(false_endpoint)
        assert response is None


class TestThirukuralApiFetchDataUploadS3:
    """This class tests for possible testcases in ThirukuralDataUploadS3"""

    def test_fetch_data_s3_object(self):
        """This method tests for instance of the class ThirukuralDataUploadS3"""
        self.obj = ThirukuralDataUploadS3()
        assert isinstance(self.obj, ThirukuralDataUploadS3)

    def test_get_response_from_api_done(self, kural_num):
        """This method tests for getting response from thirukural api for the given kural number is done"""
        self.obj = ThirukuralDataUploadS3()
        response = self.obj.get_response_from_api(kural_num)
        assert response is not None

    @pytest.mark.xfail
    def test_get_response_from_api_notdone(self, invalid_num):
        """This method tests for getting response from thirukural api for the given kural number is notdone"""
        self.obj = ThirukuralDataUploadS3()
        response = self.obj.get_response_from_api(invalid_num)
        assert response is None

    def test_create_dataframe_for_response_done(self, response, kural_num):
        """Thi smethod tests for creating dataframe for the response is done"""
        self.obj = ThirukuralDataUploadS3()
        dataframe = self.obj.get_dataframe_for_response(response, kural_num)
        assert isinstance(dataframe, pd.DataFrame)

    @pytest.mark.xfail
    def test_create_dataframe_for_response_notdone(self, false_response, invalid_num):
        """This method tests for creating dataframe for the response is notdone"""
        self.obj = ThirukuralDataUploadS3()
        dataframe = self.obj.get_dataframe_for_response(false_response, invalid_num)
        assert dataframe is None

    def test_create_json_file_done(self, kural_num, dataframe, file_name, partition_path):
        """This method tests for creating the json file for the dataframe is done"""
        self.obj = ThirukuralDataUploadS3()
        file = self.obj.create_json_file_partition(dataframe, file_name, partition_path, kural_num)
        assert file == file_name

    @pytest.mark.xfail
    def test_create_json_file_notdone(self, kural_num, file_name, partition_path):
        """This method tests for creating the json file for the dataframe is not done"""
        self.obj = ThirukuralDataUploadS3()
        dataframe = None
        file = self.obj.create_json_file_partition(dataframe, file_name, partition_path, kural_num)
        assert file is None

    def test_put_partition_path_done(self, kural_num,response, partition_path):
        """This method tests to put the partition path based on kural number is done"""
        self.obj = ThirukuralDataUploadS3()
        path = self.obj.put_partition_path(kural_num,response)
        assert path == partition_path

    @pytest.mark.xfail
    def test_put_partition_path_notdone(self, invalid_num,false_response):
        """This method tests to put the partition path based on kural number is notdone"""
        self.obj = ThirukuralDataUploadS3()
        path = self.obj.put_partition_path(invalid_num,false_response)
        assert path is None

    def test_kural_num_is_valid(self,kural_num):
        """This method checks the given kural number is valid and within the range"""
        valid_num=check_valid_kuralnumber(kural_num)
        assert valid_num is kural_num
      
    @pytest.mark.xfail  
    def test_kural_num_is_notvalid(self,invalid_num):
        """This method checks the given kural number is not valid and within the range"""
        valid_num=check_valid_kuralnumber(invalid_num)
        assert valid_num is None
        