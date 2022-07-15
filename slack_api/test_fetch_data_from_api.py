from logging.handlers import TimedRotatingFileHandler
import logging
import os
from datetime import datetime
import configparser
import pytest
import requests
import boto3
from moto import mock_s3
from aws_s3.s3_details import S3Details
from fetch_data_from_api_upload_s3 import FetchAdminAnalyticsData,check_valid_date
from get_response_from_api import SlackApi


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
    """this method returns the slack api section from config object"""
    section = config_path["slack_api"]
    return section


@pytest.fixture
def logger_obj(parent_dir, config_path):
    """This method returns the logger object of time rotating time logger"""
    log_dir = os.path.join(
        parent_dir,
        config_path["local"]["log_path"],
        "slack_api_data_test",
    )
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_file = os.path.join(log_dir, "slack_api_test.log")
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
def startdate():
    """This method returns the startdate for getting the data"""
    startdate = datetime.strptime("2022-07-13", "%Y-%m-%d").date()
    return startdate


@pytest.fixture
def enddate():
    """This method returns the enddate for getting the data"""
    enddate = datetime.strptime("2022-07-14", "%Y-%m-%d").date()
    return enddate


@pytest.fixture
def date_none():
    """This method returns the startdate as none"""
    return None


@pytest.fixture
def partition_path(startdate):
    """This method returns the partition path based on date"""
    partition_path = startdate.strftime(
        f"pt_year=%Y/pt_month=%m/pt_day=%d"
    )
    return partition_path


@pytest.fixture
def file_name(startdate):
    """This method returns the file name"""
    return f"Member_analytics_data_on_{startdate}.json"


@pytest.fixture
def wrong_file_name(date_none):
    """This method returns the  wrong file name"""
    return f"data_on_{date_none}.json"

@pytest.fixture
def encrypt_key(config_section):
    """This method returns the key to be decrypted from config"""
    key=config_section["api_key"]
    return key

@pytest.fixture
def fernet_key(config_section):
    """This method returns the fernet key from config"""
    key=config_section["fernet_key"]
    return key

@pytest.fixture
def false_key():
    """This method returns the false key to be decrypted"""
    return "SDGSDF452afdfg345DFG"

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
    base_url = "https://slack.com/api/admin.analytics.getFile"
    return base_url

@pytest.fixture
def endpoint(startdate):
    """This method returns the endpoint"""
    endpoint = f"?date={startdate}"
    return endpoint

@pytest.fixture
def false_endpoint(date_none):
    """This method returns the false endpoint"""
    endpoint = f"?date={date_none}"
    return endpoint
    
@pytest.fixture
def header():
    """This method returns the access token """
    return "xxxx-xxxxxxxxx-xxxx"

@pytest.fixture
def response(base_url, endpoint,header):
    """This method get the response from api for the given endpoint"""
    request_url = base_url + endpoint
    response = requests.get(request_url,header=header)
    if response.status_code == 200:
        response_json =response 
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
            response_json =response
        else:
            response_json = None
    except Exception as err:
        response_json = None
    return response_json

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
        
class TestSlackApi:
    """This class has methods to test possible testcases in class SlackApi"""

    def test_slack_api_object(self, config_section, logger_obj):
        """This method tests the instance for the slack api"""
        self.api = SlackApi(config_section, logger_obj)
        assert isinstance(self.api, SlackApi)
    
    def test_decrypt_api_key_is_done(self,logger_obj,config_section,encrypt_key,fernet_key):
        """This method tests the decryption of api key from config file is done"""
        self.api = SlackApi (logger_obj,config_section)
        api_key=self.api.decrypt_api_key_from_config(encrypt_key,fernet_key)
        assert isinstance(api_key,str)
    
    @pytest.mark.xfail
    def test_decrypt_api_key_is_notdone(self,logger_obj,config_section,encrypt_key,false_key):
        """This method tests the decryption of api key from config file is  not done"""
        self.api = SlackApi (logger_obj,config_section)
        api_key=self.api.decrypt_api_key_from_config(encrypt_key,false_key)
        assert api_key is None
    
    def test_authenticate_api_key_is_done(self,logger_obj,config_section):
        """This method tests the apikey is authenticated and header is created"""
        self.api = SlackApi (logger_obj,config_section)
        header=self.api.authenticate_api_with_token()
        assert header is not None
    
    def test_get_endpoint_for_object_record_done(self, config_section, logger_obj,startdate ):
        """This method tests for getting endpoint for fetching analytics data from api"""
        self.api = SlackApi(config_section, logger_obj)
        endpoint = self.api.get_endpoint_for_admin_analytics_data(startdate)
        assert endpoint is not None

    @pytest.mark.xfail
    def test_get_endpoint_for_object_record_notdone(self, config_section, logger_obj,date_none):
        """This method tests for getting endpoint for fetching analytics data from api is failed"""
        self.api = SlackApi(config_section, logger_obj)
        endpoint = self.api.get_endpoint_for_admin_analytics_data(date_none)
        assert endpoint is None

    def test_get_response_from_api_done(self, config_section, logger_obj, endpoint):
        """This method tests for getting the response from api for the given endpoint is done"""
        self.api = SlackApi(config_section, logger_obj)
        response = self.api.get_response_from_api(endpoint)
        assert response is not None

    @pytest.mark.xfail
    def test_get_response_from_api_notdone(self, config_section, logger_obj, false_endpoint):
        """This method tests for getting the response from api for the given endpoint is failed"""
        self.api = SlackApi(config_section, logger_obj)
        response = self.api.get_response_from_api(false_endpoint)
        assert response is None

        
class TestFetchAdminAnalyticsData:
    """This class tests for possible testcases in FetchAdminAnalyticsData"""

    def test_fetch_record_s3_object(self, startdate, enddate):
        """This method tests for instance of the class FetchAdminAnalyticsData"""
        self.obj = FetchAdminAnalyticsData(startdate, enddate)
        assert isinstance(self.obj, FetchAdminAnalyticsData)
    
    def test_get_admin_analytics_from_api_done(self,enddate,startdate):
        """This method tests for getting the response from api for the given dates"""
        self.obj = FetchAdminAnalyticsData(startdate, enddate)
        response=self.obj.get_admin_analytics_from_api(startdate,enddate)
        assert response is not None
        
    def test_get_admin_analytics_from_api_notdone(self,enddate,startdate):
        """This method tests for getting the response from api for the given dates is failed"""
        self.obj = FetchAdminAnalyticsData(startdate, enddate)
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            response=self.obj.get_admin_analytics_from_api(startdate,enddate)
        assert response is None
        assert pytest_wrapped_e.type == SystemExit
        assert pytest_wrapped_e.value.code == 42
        
    def test_decompress_json_response_done(self,response,startdate, enddate):
        """This method tests for decompressing the json response is done"""
        self.obj = FetchAdminAnalyticsData(startdate, enddate)
        file=self.obj.decompress_json_response(response)
        assert file is not None
        
    @pytest.mark.xfail
    def test_decompress_json_response_notdone(self,false_response,startdate, enddate):
        """This method tests for decompressing the json response is not done"""
        self.obj = FetchAdminAnalyticsData(startdate, enddate)
        file=self.obj.decompress_json_response(false_response)
        assert file is None
        
    def test_put_partition_path_done(self, partition_path, startdate, enddate):
        """This method tests to put the partition path based on date is done"""
        self.obj = FetchAdminAnalyticsData(startdate, enddate)
        path = self.obj.put_partition_path(startdate)
        assert path == partition_path

    @pytest.mark.xfail
    def test_put_partition_path_notdone(self, startdate, date_none):
        """This method tests to put the partition path based on date is notdone"""
        self.obj = FetchAdminAnalyticsData(startdate, date_none)
        path = self.obj.put_partition_path(date_none)
        assert path is None
    
    def test_check_date_is_valid(self, startdate):
        """This method checks the given date is in a valid format"""
        startdate = datetime.strftime(startdate, "%Y-%m-%d")
        valid_date = check_valid_date(startdate)
        date = datetime.strptime(startdate, "%Y-%m-%d").date()
        assert isinstance(valid_date,datetime)

    @pytest.mark.xfail
    def test_check_date_is_notvalid(self):
        """This method checks the given date is not in a valid format"""
        valid_date = check_valid_date("10-06-2022")
        assert not isinstance(valid_date,datetime)