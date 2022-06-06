"""This module tests for possible testcases in fetching data from api based
on endpoint, s3 module, partition and upload module"""
from logging.handlers import TimedRotatingFileHandler
import logging
from cryptography.fernet import Fernet
import configparser
from datetime import datetime
import py 
import pytest
import pandas as pd
import os
import requests
import boto3
from moto import mock_s3
from sqlalchemy import JSON
from fetch_data_partition_upload_s3 import HistoricDataProductsUploadS3
from fetch_data_product_upload_local import ApiDataPartitionUploadLocal
from get_response_from_api_with_authentication import PullDataFromEBirdApi
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
    section=config_path["eBird_api_datas"]
    return section

@pytest.fixture
def logger_obj(parent_dir, config_path):
    """This method returns the logger object of time rotating time logger"""
    log_dir = os.path.join(
        parent_dir,
        config_path["local"]["log_path"],
        "eBird_api_data_test",
    )
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_file = os.path.join(log_dir, "eBirdApitest.log")
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
    local_path = os.path.join(parent_dir, config_path["local"]["data_path"], "eBird_api_data_test")
    if not os.path.exists(local_path):
        os.makedirs(local_path)
    return local_path

@pytest.fixture
def startdate():
    """This method returns the start date"""
    date="2022-06-05"
    cr_date = datetime.strptime(date, '%Y-%m-%d').date()
    return cr_date

@pytest.fixture
def enddate():
    """This method returns the end date"""
    date="2022-06-06"
    cr_date = datetime.strptime(date, '%Y-%m-%d').date()
    return cr_date

@pytest.fixture
def dates(startdate,enddate):
    """This method returns the dict of startdate and enddate"""
    dates={'date1':startdate,'date2':enddate}
    return dates

@pytest.fixture
def date_none():
    """This method returns the date as None"""
    return None

@pytest.fixture
def region_list():
    """This method returns the list of regions"""
    return ['IN','US','AU']

@pytest.fixture
def region_code():
    """This method returns the region_code on which datas are needed"""
    return 'IN'

@pytest.fixture
def new_region():
    """This method returns the new_region for which datas are needed"""
    return 'MX'

@pytest.fixture
def false_endpoint(region_code,strip_date):
    """This method returns the false endpoint"""
    endpoint = (
                f"datsd/sjh/{region_code}/{strip_date.year}/{strip_date.month}/{strip_date.day}"
            )
    return endpoint

@pytest.fixture
def endpoint():
    """This method returns the endpoint for historical observations"""
    return 'historical_data'

@pytest.fixture
def endpoint_value():
    """This method returns the endpoint for historical observations"""
    return 'data/obs'

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

@pytest.fixture
def partition_path(bucket_path,endpoint,startdate,region_code):
    """This method returns the partition path based on date and endpoint"""
    partition_path=startdate.strftime(
                f"{bucket_path}/{endpoint}/pt_region={region_code}/pt_year=%Y/pt_month=%m/pt_day=%d"
            )
    return partition_path

@pytest.fixture
def strip_date(startdate):
    """This method strips the date in year,month and date format"""
    return datetime.strptime(str(startdate), "%Y-%m-%d")

@pytest.fixture
def hist_endpoint(endpoint_value,region_code,strip_date):
    """This method returns the endpoint for historical observations from eBirdApi"""
    endpoint = (
                f"{endpoint_value}/{region_code}/historic/{strip_date.year}/{strip_date.month}/{strip_date.day}"
            )
    return endpoint

@pytest.fixture
def checklist_endpoint(strip_date,region_code):
    """This method returns the endpoint for Checklist feed from eBirdApi"""
    endpoint = (
                f"product/lists/{region_code}/{strip_date.year}/{strip_date.month}/{strip_date.day}"
            )
    return endpoint

@pytest.fixture
def top100_endpoint(region_code,strip_date):
    """This method returns the endpoint for top100 contributors from eBirdApi"""
    endpoint = (
                f"product/top100/{region_code}/{strip_date.year}/{strip_date.month}/{strip_date.day}"
            )
    return endpoint

@pytest.fixture
def file_name(endpoint,startdate):
    """This method returns the file name"""
    return f"{endpoint}_on_{startdate}.json"

@pytest.fixture
def wrong_file_name(endpoint,date_none):
    """This method returns the  wrong file name"""
    return f"{endpoint}_on_{date_none}.json"

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
def s3_upload(s3_mock_client,bucket_name,s3_bucket,file,bucket_path):
    """This fixture is to upload file in s3 mock bucket"""
    upload=s3_mock_client.put_object(Bucket=bucket_name, Body=file, Key=bucket_path)
    yield 
    
    
@pytest.fixture
def header(config_path):
    """This method returns the header for the api key authentication"""
    encrypt_key =config_path["eBird_api_datas"]["api_key"]
    fernet_key = config_path["eBird_api_datas"]["fernet_key"]
    fernet = Fernet(fernet_key)
    decrypt_token = fernet.decrypt(encrypt_key.encode()).decode()
    header = {"X-eBirdApiToken": decrypt_token}
    return header
    
@pytest.fixture
def base_url():
    """This method returns the base url of the api"""
    base_url="https://api.ebird.org/v2/"
    return base_url
    
@pytest.fixture
def response(base_url,endpoint_value,header,region_code,startdate):
    """This method get the response from api for the given endpoint"""
    get_date = datetime.strptime(str(startdate), "%Y-%m-%d")
    endpoints = (
                f"{endpoint_value}/{region_code}/historic/{get_date.year}/{get_date.month}/{get_date.day}"
            )
    request_url=base_url+endpoints
    response=requests.get(request_url,headers=header).json()
    return response

@pytest.fixture
def dataframe(response):
    """This method returns the dataframe from the response"""
    return pd.DataFrame(response)
    
    
@pytest.fixture
def copy_source(upload_path,file_name):
    """This method creates and returns the temporary copy source"""
    return upload_path+"/"+file_name
     
class Test_S3:
    """This class will test all possible testcases in s3 module"""
    
    def test_s3_object(self,logger_obj,config_path):
        """This method test the instance belong to the class of S3Service"""
        self.s3_obj = S3Details(logger_obj,config_path)
        assert isinstance(self.s3_obj, S3Details)
        
    def test_upload_file_s3_done(self,file,config_path,key,bucket_name,s3_mock_client,s3_bucket,bucket_path,logger_obj):
        """This method will test file is sucessfully uploaded"""
        self.my_client = S3Details(logger_obj,config_path)
        file = self.my_client.upload_file(file,bucket_name,bucket_path,key)
        assert file is not None
        
    @pytest.mark.xfail
    def test_upload_file_S3_notdone(self,config_path,key,no_file,bucket_name,s3_mock_client,s3_bucket,bucket_path,logger_obj):
        """This method will test file is not uploaded"""
        self.my_client = S3Details(logger_obj,config_path)
        file= self.my_client.upload_file(no_file,bucket_name,bucket_path,key)
        assert file is None
        
class Test_UploadLocal:
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
        upload_path,
        copy_source,
        file_name,
        endpoint,
        partition_path,
        startdate,
    ):
        """This method will test for the nobelprize details is written to local file
        in class  ApiDataPartitionUploadLocal"""
        self.obj_local = ApiDataPartitionUploadLocal(logger_obj)
        dataframe.to_json(upload_path + "/" + file_name, orient="records", lines=True)
        local_file = self.obj_local.upload_partition_s3_local(
            upload_path, copy_source, file_name, partition_path
        )
        assert local_file == f"{endpoint}_on_{startdate}.json"

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
        """This method will test for nobelprize details is not written to local file
        in class  ApiDataPartitionUploadLocal"""
        self.obj_local = ApiDataPartitionUploadLocal(logger_obj)
        dataframe.to_json(upload_path + "/" + wrong_file_name, orient="records", lines=True)
        local_file = self.obj_local.upload_partition_s3_local(
            upload_path, copy_source, wrong_file_name, partition_path
        )
        assert local_file is None
    
class Test_EBirdApi:
    """This class tests for possible testcases in fetching data from api in class PullDataFromEBirdApi"""
    
    def test_ebird_api_object(self,logger_obj,config_section):
        """This method tests the instance of the class PullDataFromEBirdApi"""
        self.api = PullDataFromEBirdApi (logger_obj,config_section)
        assert isinstance(self.api,  PullDataFromEBirdApi)
    
    def test_decrypt_api_key_is_done(self,logger_obj,config_section,encrypt_key,fernet_key):
        """This method tests the decryption of api key from config file is done"""
        self.api = PullDataFromEBirdApi (logger_obj,config_section)
        api_key=self.api.decrypt_api_key_from_config(encrypt_key,fernet_key)
        assert isinstance(api_key,str)
    
    @pytest.mark.xfail
    def test_decrypt_api_key_is_notdone(self,logger_obj,config_section,encrypt_key,false_key):
        """This method tests the decryption of api key from config file is  not done"""
        self.api = PullDataFromEBirdApi (logger_obj,config_section)
        api_key=self.api.decrypt_api_key_from_config(encrypt_key,false_key)
        assert api_key is None
    
    def test_authenticate_api_key_is_done(self,logger_obj,config_section):
        """This method tests the apikey is authenticated and header is created"""
        self.api = PullDataFromEBirdApi (logger_obj,config_section)
        header=self.api.authenticate_api_with_key()
        assert header is not None
        
    def test_fetch_historical_data_from_api_isdone(self,logger_obj,config_section,region_code,hist_endpoint):
        """This method tests to fetch historical observations from api is done"""
        self.api = PullDataFromEBirdApi (logger_obj,config_section)
        response=self.api.get_response_from_endpoint(hist_endpoint,region_code)
        assert isinstance(response,list)

    @pytest.mark.xfail
    def test_fetch_historical_data_from_api_notdone(self,logger_obj,config_section,false_endpoint,region_code):
        """This method tests to fetch historical observations from api is not done"""
        self.api = PullDataFromEBirdApi (logger_obj,config_section)
        response=self.api.get_response_from_endpoint(false_endpoint,region_code)
        assert response is None
    
    def test_fetch_top100_contriutors_from_api_isdone(self,logger_obj,config_section,region_code,top100_endpoint):
        """This method tests to fetch top100_contriutors from api is done"""
        self.api = PullDataFromEBirdApi (logger_obj,config_section)
        response=self.api.get_response_from_endpoint(top100_endpoint,region_code)
        assert isinstance(response,list)

    @pytest.mark.xfail
    def test_fetch_top100_contriutors_from_api_notdone(self,logger_obj,config_section,false_endpoint,region_code):
        """This method tests to fetch top100_contriutors from api is not done"""
        self.api = PullDataFromEBirdApi (logger_obj,config_section)
        response=self.api.get_response_from_endpoint(false_endpoint,region_code)
        assert response is None
    
    def test_fetch_checklist_feed_from_api_isdone(self,logger_obj,config_section,region_code,checklist_endpoint):
        """This method tests to fetch checklist_feed from api is done"""
        self.api = PullDataFromEBirdApi (logger_obj,config_section)
        response=self.api.get_response_from_endpoint(checklist_endpoint,region_code)
        assert isinstance(response,list)

    @pytest.mark.xfail
    def test_fetch_checklist_feed_from_api_notdone(self,logger_obj,config_section,false_endpoint,region_code):
        """This method tests to fetch checklist_feed from api is not done"""
        self.api = PullDataFromEBirdApi (logger_obj,config_section)
        response=self.api.get_response_from_endpoint(false_endpoint,region_code)
        assert response is None
    
class Test_HistoricDataProductUploadS3():
    """This class tests the possible testcases in HistoricDataProductsUploadS3 class"""
    
    def test_fetch_historic_product_datas_object(self,startdate,enddate,region_code):
        """This method tests the instance of the class HistoricDataProductsUploadS3"""
        self.obj = HistoricDataProductsUploadS3(startdate, enddate,region_code)
        assert isinstance(self.obj,HistoricDataProductsUploadS3)
    
    def test_check_for_regions_available_done(self,startdate,enddate,region_code,config_section,dates):
        """This method tests for the given region is available in the config regions"""
        available_regions = config_section["region"]
        code_list=[]
        code_list=available_regions.split(",")
        self.obj = HistoricDataProductsUploadS3(startdate, enddate,region_code)
        region_code=self.obj.check_for_regions_available(dates)
        assert region_code==code_list
    
    def test_get_data_from_eachdate_isdone(self,startdate,enddate,region_code,region_list):
        """This method tests for getting data for single date between the given dates"""
        self.obj = HistoricDataProductsUploadS3(startdate, enddate,region_code)
        date=self.obj.get_data_from_api_for_eachdate(enddate,startdate,region_list)
        assert date==enddate
        
    @pytest.mark.xfail
    def test_get_data_from_eachdate_is_notdone(self,date_none,enddate,region_code,region_list):
        """This method tests for getting data for single date between the given dates is failed"""
        self.obj = HistoricDataProductsUploadS3(date_none, enddate,region_code)
        date=self.obj.get_data_from_api_for_eachdate(enddate,date_none,region_list)
        assert date is None
        
    def test_fetch_datframe_from_api_done(self,startdate,enddate,region_code,endpoint_value,endpoint):
        """This method tests for getting the historical observation from api and dataframe is created for the response"""
        self.obj = HistoricDataProductsUploadS3(startdate, enddate,region_code)
        df_data=self.obj.fetch_dataframe_from_api_for_endpoints(region_code,startdate,endpoint,endpoint_value)
        assert df_data is not None
    
    @pytest.mark.xfail
    def test_fetch_datframe_from_api_notdone(self,startdate,enddate,region_code,endpoint_value,false_endpoint):
        """This method tests for getting the historical observation from api and dataframe is not created for the response"""
        self.obj = HistoricDataProductsUploadS3(startdate, enddate,region_code)
        df_data=self.obj.fetch_dataframe_from_api_for_endpoints(region_code,startdate,false_endpoint,endpoint_value)
        assert df_data is None
    
    def test_create_json_for_dataframe_isdone(self,startdate,enddate,region_code,dataframe,endpoint,file_name):
        """This method tests the creation of json file for given dataframe is done """
        self.obj = HistoricDataProductsUploadS3(startdate, enddate,region_code)
        file=self.obj.create_json_upload_s3(dataframe,startdate,region_code,endpoint)
        assert file==file_name
    
    @pytest.mark.xfail
    def test_create_json_for_dataframe_is_notdone(self,startdate,enddate,region_code,dataframe,false_endpoint,file_name):
        """This method tests the creation of json file for given dataframe is not done """
        self.obj = HistoricDataProductsUploadS3(startdate, enddate,region_code)
        file=self.obj.create_json_upload_s3(dataframe,startdate,region_code,false_endpoint)
        assert file is None 
    
    def test_put_partition_path_isdone(self,startdate, enddate,region_code,endpoint,partition_path,bucket_path):
        """This method tests the partition path is based on region, year, month and date"""
        self.obj = HistoricDataProductsUploadS3(startdate, enddate,region_code)
        path=self.obj.put_partition_path(bucket_path,region_code,startdate,endpoint)
        assert path==partition_path
    
    @pytest.mark.xfail
    def test_put_partition_path_is_notdone(self,startdate, date_none,region_code,endpoint,partition_path,bucket_path):
        """This method tests the partition path is based on region, year, month and date is not made"""
        self.obj = HistoricDataProductsUploadS3(startdate,date_none,region_code)
        path=self.obj.put_partition_path(bucket_path,region_code,date_none,endpoint)
        assert path is None
        
    def test_exceution_last_date_isupdated(self, startdate,date_none,region_code,config_section):
        """This method tests for the last date of execution is updated in config file"""
        self.obj = HistoricDataProductsUploadS3(startdate,date_none,region_code)
        self.obj.last_date_of_execution()
        last_date = config_section.get("last_run_date")
        assert last_date == str(datetime.now().date())

    @pytest.mark.xfail
    def test_exceution_last_date_is_notupdated(self, startdate, enddate,region_code):
        """This method tests for the last date of execution is updated in config file"""
        self.obj = HistoricDataProductsUploadS3( startdate, enddate,region_code)
        last_date=self.obj.last_date_of_execution()
        assert last_date is None
    
    def test_update_config_for_region_done(self,startdate,enddate,new_region,config_section):
        """This method tests for updating the config if new region is given"""
        self.obj = HistoricDataProductsUploadS3(startdate, enddate,new_region)
        self.obj.update_config_if_region_not_exixts()
        regions = config_section.get("region")
        assert new_region in regions
        
    @pytest.mark.xfail
    def test_update_config_for_region_notdone(self,startdate,enddate,region_code,config_section):
        """This method tests for updating the config if new region is not given"""
        self.obj = HistoricDataProductsUploadS3(startdate, enddate,region_code)
        regions=self.obj.update_config_if_region_not_exixts()
        assert regions is None
        
        