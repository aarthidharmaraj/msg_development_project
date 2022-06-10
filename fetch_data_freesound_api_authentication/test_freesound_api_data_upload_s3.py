"""This module tests for possible testcases in fetching data from free sound api based
on endpoint, s3 module, partition and upload module"""
from logging.handlers import TimedRotatingFileHandler
import logging
import os
from datetime import datetime
import configparser
from cryptography.fernet import Fernet
import pytest
import pandas as pd
import requests
import boto3
from moto import mock_s3
from fetch_data_from_api_partition_upload_s3 import FetchDataFromApiUploadS3
from fetch_data_partition_upload_local import ApiDataPartitionUploadLocal
from get_response_from_api import PullDataFromFreeSoundApi
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
    """this method returns the ebird api section from config object"""
    section = config_path["freesound_api_datas"]
    return section


@pytest.fixture
def logger_obj(parent_dir, config_path):
    """This method returns the logger object of time rotating time logger"""
    log_dir = os.path.join(
        parent_dir,
        config_path["local"]["log_path"],
        "freesound_api_data_test",
    )
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_file = os.path.join(log_dir, "freesoundApitest.log")
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
    local_path = os.path.join(parent_dir, config_path["local"]["data_path"], "f_api_data_test")
    if not os.path.exists(local_path):
        os.makedirs(local_path)
    return local_path


@pytest.fixture
def date():
    """This method returns the date"""
    date = "2022-06-05"
    cr_date = datetime.strptime(date, "%Y-%m-%d").date()
    return cr_date


@pytest.fixture
def date_none():
    """This method returns the date as None"""
    return None


@pytest.fixture
def soundid():
    """This method returns the sound id for getting similar sounds"""
    return 636908


@pytest.fixture
def username():
    """This method returns the username for getting user packs"""
    return "Jovica"


@pytest.fixture
def wrong_username():
    """This method returns the wrong username for getting user packs"""
    return "sdj"


@pytest.fixture
def false_endpoint(wrong_username):
    """This method returns the false endpoint"""
    endpoint = f"users/{wrong_username}/packs/"
    return endpoint


@pytest.fixture
def endpoint(username):
    """This method returns the endpoint for user packs"""
    endpoint = f"users/{username}/packs/"
    return endpoint


@pytest.fixture
def endpoint_key():
    """This method returns the endpoint key for user packs"""
    return "user_packs"


@pytest.fixture
def encrypt_key(config_section):
    """This method returns the key to be decrypted from config"""
    key = config_section["api_key"]
    return key


@pytest.fixture
def fernet_key(config_section):
    """This method returns the fernet key from config"""
    key = config_section["fernet_key"]
    return key


@pytest.fixture
def false_key():
    """This method returns the false key to be decrypted"""
    return "SDGSDF452afdfg345DFG"


@pytest.fixture
def partition_path(date):
    """This method returns the partition path based on date and endpoint"""
    partition_path = date.strftime(f"pt_year=%Y/pt_month=%m/pt_day=%d")
    return partition_path


@pytest.fixture
def file_name(username):
    """This method returns the file name"""
    return f"{username}_116.json"


@pytest.fixture
def wrong_file_name(endpoint, date_none):
    """This method returns the  wrong file name"""
    return f"{date_none}.json"


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
def params(config_section):
    """This method returns the header for the api key authentication"""
    encrypt_key = config_section["api_key"]
    fernet_key = config_section["fernet_key"]
    fernet = Fernet(fernet_key)
    decrypt_token = fernet.decrypt(encrypt_key.encode()).decode()
    params = {"token": decrypt_token}
    return params


@pytest.fixture
def base_url():
    """This method returns the base url of the api"""
    base_url = "https://freesound.org/apiv2/"
    return base_url


@pytest.fixture
def response(base_url, username, params):
    """This method get the response from api for the given endpoint"""
    endpoint = f"users/{username}/packs/"
    request_url = base_url + endpoint
    print(request_url)
    response = requests.get(request_url, params=params).json()
    return response


@pytest.fixture
def dataframe(config_section,logger_obj,endpoint):
    """This method returns the dataframe from the response"""
    obj=PullDataFromFreeSoundApi(config_section,logger_obj)
    dataframe=obj.get_response_from_api_pagination(endpoint)
    return dataframe

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
        assert local_file ==file_name

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


class TestFreeSoundApi:
    """This class tests for possible testcases in fetching data from api in
    class PullDataFromFreeSoundApi"""

    def test_ebird_api_object(
        self,
        config_section,
        logger_obj,
    ):
        """This method tests the instance of the class PullDataFromFreeSoundApi"""
        self.api = PullDataFromFreeSoundApi(config_section, logger_obj)
        assert isinstance(self.api, PullDataFromFreeSoundApi)

    def test_decrypt_api_key_is_done(self, config_section, logger_obj, encrypt_key, fernet_key):
        """This method tests the decryption of api key from config file is done"""
        self.api = PullDataFromFreeSoundApi(config_section, logger_obj)
        api_key = self.api.decrypt_api_key_from_config(encrypt_key, fernet_key)
        assert isinstance(api_key, str)

    @pytest.mark.xfail
    def test_decrypt_api_key_is_notdone(self, config_section, logger_obj, encrypt_key, false_key):
        """This method tests the decryption of api key from config file is  not done"""
        self.api = PullDataFromFreeSoundApi(config_section, logger_obj)
        api_key = self.api.decrypt_api_key_from_config(encrypt_key, false_key)
        assert api_key is None

    def test_authenticate_api_key_is_done(self, config_section, logger_obj):
        """This method tests the apikey is authenticated and header is created"""
        self.api = PullDataFromFreeSoundApi(config_section, logger_obj)
        params = self.api.authenticate_api_with_key()
        assert params is not None

    def test_fetch_user_packs_from_api_done(self, config_section, logger_obj, username):
        """This method tests for fetching the details of user packs from api based on the username is done"""
        self.api = PullDataFromFreeSoundApi(config_section, logger_obj)
        dataframe = self.api.fetch_user_sounds_packs_from_api(username)
        assert isinstance(dataframe, pd.DataFrame)

    @pytest.mark.xfail
    def test_fetch_user_packs_from_api_notdone(self,config_section,logger_obj,wrong_username):
        """This method tests for fetching the details of user packs from api based on the username is notdone"""
        self.api=PullDataFromFreeSoundApi(config_section,logger_obj)
        dataframe=self.api.fetch_user_sounds_packs_from_api(wrong_username)
        assert dataframe.empty


class TestFreeSoundApiUploadS3:
    """This class has methods to test possible testcases in FetchDataFromApiUploadS3"""

    def test_freesoundapi_object(self, soundid, username, endpoint):
        self.obj = FetchDataFromApiUploadS3(soundid, username, endpoint)
        assert isinstance(self.obj, FetchDataFromApiUploadS3)

    def test_fetch_dataframe_for_endpoints_done(self, soundid, username, endpoint_key):
        """This method tests for getting the dataframe for the given endpoints is done"""
        self.obj = FetchDataFromApiUploadS3(soundid, username, endpoint_key)
        response = self.obj.fetch_response_from_api_for_endpoints()
        assert response is not None

    @pytest.mark.xfail
    def test_fetch_dataframe_for_endpoints_notdone(self, soundid, username, false_endpoint):
        """This method tests for getting the dataframe for the given endpoints is not done"""
        self.obj = FetchDataFromApiUploadS3(soundid, username, false_endpoint)
        response = self.obj.fetch_response_from_api_for_endpoints()
        assert response is None

    def test_get_date_from_dataframe_done(self,soundid,username,dataframe,endpoint):
        """This method tests for getting date from dataframe is done"""
        self.obj=FetchDataFromApiUploadS3(soundid,username,endpoint)
        date=self.obj.get_date_from_dataframe(dataframe)
        assert isinstance(date,pd.DataFrame)

    @pytest.mark.xfail
    def test_get_date_from_dataframe_notdone(self, soundid, username, endpoint):
        """This method tests for getting date from dataframe is not done"""
        self.obj = FetchDataFromApiUploadS3(soundid, username, endpoint)
        dataframe = pd.DataFrame()
        date = self.obj.get_date_from_dataframe(dataframe)
        assert date is None

    def test_create_json_upload_s3_done(
        self, soundid, username, endpoint, dataframe, file_name, date
    ):
        """This method tests for creating the json file for the given dataframe is done and uploaded in the give path"""
        self.obj = FetchDataFromApiUploadS3(soundid, username, endpoint)
        file = self.obj.create_json_upload_s3(dataframe, file_name, str(date))
        assert file == file_name

    @pytest.mark.xfail
    def test_create_json_upload_s3_notdone(
        self, soundid, username, endpoint, dataframe, file_name, date_none
    ):
        """This method tests for creating the json file for the given dataframe is done and uploaded in the give path"""
        self.obj = FetchDataFromApiUploadS3(soundid, username, endpoint)
        file = self.obj.create_json_upload_s3(dataframe, file_name, date_none)
        assert file is None

    def test_put_partition_path_done(self, soundid, username, endpoint, date, partition_path):
        """This method tests the partition path is done based on the given date"""
        self.obj = FetchDataFromApiUploadS3(soundid, username, endpoint)
        path = self.obj.put_partition_path(str(date))
        assert path == partition_path

    @pytest.mark.xfail
    def test_put_partition_path_notdone(self, soundid, username, endpoint, date_none):
        """This method tests the partition path is done based on the given date"""
        self.obj = FetchDataFromApiUploadS3(soundid, username, endpoint)
        path = self.obj.put_partition_path(date_none)
        assert path is None
