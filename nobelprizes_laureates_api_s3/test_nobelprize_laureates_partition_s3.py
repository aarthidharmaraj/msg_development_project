import pytest
from datetime import datetime
from time import time
import logging
import os
import pandas as pd
import sys
import boto3
import configparser
from moto import mock_s3
from nobelprize_laureates_partition_s3 import NobelPrizeLaureatesPartitionS3
from nobelprizes_laureates_upload_local import NobelLaureateLocalUpload
from pull_nobelprizes_laureates_from_api import NobelprizeLaureatesFromApi
from aws_s3.s3_details import S3Details

parent_dir = os.path.dirname(os.getcwd())
config = configparser.ConfigParser()
config.read(parent_dir + "/details.ini")


@pytest.fixture
def start_year():
    """This method returns the start year"""
    return 2019


@pytest.fixture
def end_year():
    """This method returns the end year"""
    return 2021


@pytest.fixture
def endpoint_prize():
    """This method returns the endpoint for nobelPrizes"""
    return "nobelPrizes"


@pytest.fixture
def endpoint_laureate():
    """This method returns the endpoint for laureates"""
    return "laureates"


@pytest.fixture
def base_url():
    """his method returns the base url of api"""
    url = config["nobelprize_laureates_api"]["basic_url"]
    return url


@pytest.fixture
def year_none():
    """This method returns the date as none"""
    return None


@pytest.fixture
def file_name(start_year):
    """This method returns the file name"""
    return f"pull_for_{start_year}.json"


@pytest.fixture
def wrong_file_name(year_none):
    """This method returns the wrong file name"""
    return f"pull_for_{year_none}.json"


@pytest.fixture
def partition_for_prize(start_year):
    """This method returns the partition path"""
    partition_path = f"nobel/source/prize/pt_year={start_year}"
    return partition_path


@pytest.fixture
def wrong_partition_prize(year_none):
    """This method returns the wrong partition path for nobelprize"""
    partition_path = f"nobel/source/prize/pt_year={year_none}"
    return partition_path


@pytest.fixture
def wrong_partition_laureate(year_none):
    """This method returns the wrong partition path for laureates"""
    partition_path = f"nobel/source/laureate/pt_year={year_none}"
    return partition_path


@pytest.fixture
def partition_for_laureates(start_year):
    """This method returns the partition path"""
    partition_path = f"nobel/source/laureate/pt_year={start_year}"
    return partition_path


@pytest.fixture
def logger_obj():
    """This method returns the logger object"""
    log_dir = os.path.join(parent_dir, config["local"]["log_path"], "nobel_laureate_api_log")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_file = os.path.join(log_dir, "nobel_laureate_logfile.log")
    logging.basicConfig(
        level=logging.INFO,
        filename=log_file,
        datefmt="%d-%b-%y %H:%M:%S",
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
        filemode="w",
    )
    logger_obj = logging.getLogger()
    return logger_obj


@pytest.fixture
def upload_path():
    """This method returns the sql path"""
    local_path = os.path.join(parent_dir, config["local"]["data_path"], "nobel_laureates_data_test")
    if not os.path.exists(local_path):
        os.makedirs(local_path)
    return local_path


@pytest.fixture
def key(partition_for_prize, file_name):
    key_name = partition_for_prize + file_name
    return key_name


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture(scope="function")
def s3_mock(aws_credentials):
    with mock_s3():
        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture
def file(file_name, upload_path):
    file = os.path.join(parent_dir, upload_path, "nobel_laureates_data_test/", file_name)
    return file


@pytest.fixture
def no_file():
    return None


@pytest.fixture
def bucket_name():
    bucket_name = config["s3"]["bucket_name"]
    return bucket_name


@pytest.fixture
def bucket_path():
    bucket_path = config["s3"]["bucket_path"]
    return bucket_path


@pytest.fixture
def copy_source(upload_path, file_name):
    """This method returns the copy source to upload in local"""
    source = upload_path + "/" + file_name
    return source


@pytest.fixture
def dataframe():
    """This method returns the dataframe"""
    data = {"id": [1, 2, 3], "name": ["name1", "name2", "name3"], "awardYear": [2019, 2020, 2021]}
    return pd.DataFrame(data)


@pytest.fixture
def empty_dataframe():
    """This method returns the empty dataframe"""
    return pd.DataFrame()


@pytest.fixture
def year_list():
    """This method returns the years in a list"""
    return [2019, 2020, 2021]


@pytest.fixture
def dummy_endpoint():
    """this method returns the dummy endpoint"""
    return ""


class TestUploadPartitionLocal:
    """This class has methods to test possible testcases in
    NobelLaureateLocalUpload for nobelprizes and laureates upload local module"""

    def test_parition_local_object(self, logger_obj):
        """This method tests the instance of class NobelLaureateLocalUpload"""
        self.obj_local = NobelLaureateLocalUpload(logger_obj)
        assert isinstance(self.obj_local, NobelLaureateLocalUpload)

    def test_write_prize_to_local_file_is_done(
        self,
        logger_obj,
        dataframe,
        upload_path,
        copy_source,
        file_name,
        partition_for_prize,
        start_year,
    ):
        """This method will test for the nobelprize details is written to local file
        in class  NobelLaureateLocalUpload"""
        self.obj_local = NobelLaureateLocalUpload(logger_obj)
        dataframe.to_json(upload_path + "/" + file_name, orient="records", lines=True)
        local_file = self.obj_local.upload_parition_s3_local(
            upload_path, copy_source, file_name, partition_for_prize
        )
        assert local_file == f"pull_for_{start_year}.json"

    @pytest.mark.xfail
    def test_write_prize_to_local_file_is_not_done(
        self,
        logger_obj,
        dataframe,
        upload_path,
        copy_source,
        wrong_file_name,
        wrong_partition_prize,
    ):
        """This method will test for nobelprize details is not written to local file
        in class  NobelLaureateLocalUpload"""
        self.obj_local = NobelLaureateLocalUpload(logger_obj)
        dataframe.to_json(upload_path + "/" + wrong_file_name, orient="records", lines=True)
        local_file = self.obj_local.upload_parition_s3_local(
            upload_path, copy_source, wrong_file_name, wrong_partition_prize
        )
        assert local_file is None

    def test_write_laureates_to_local_file_is_done(
        self,
        logger_obj,
        dataframe,
        upload_path,
        copy_source,
        file_name,
        partition_for_laureates,
        start_year,
    ):
        """This method will test for the laureates details is written to local file
        in class  NobelLaureateLocalUpload"""
        self.obj_local = NobelLaureateLocalUpload(logger_obj)
        dataframe.to_json(upload_path + "/" + file_name, orient="records", lines=True)
        local_file = self.obj_local.upload_parition_s3_local(
            upload_path, copy_source, file_name, partition_for_laureates
        )
        assert local_file == f"pull_for_{start_year}.json"

    @pytest.mark.xfail
    def test_write_laureates_to_local_file_is_not_done(
        self,
        logger_obj,
        upload_path,
        dataframe,
        copy_source,
        wrong_file_name,
        wrong_partition_laureate,
    ):
        """This method will test for laureates details is not written to local file
        in class  NobelLaureateLocalUpload"""
        self.obj_local = NobelLaureateLocalUpload(logger_obj)
        dataframe.to_json(upload_path + "/" + wrong_file_name, orient="records", lines=True)
        local_file = self.obj_local.upload_parition_s3_local(
            upload_path, copy_source, wrong_file_name, wrong_partition_laureate
        )
        print(local_file)
        assert local_file is None


class TestS3Details:
    """This Test Class will check for all the possible
    Test cases in S3Operations"""

    def test_s3operations_objects(self, logger_obj):
        """This Method will test for the instance belong to the class S3Operations"""
        self.obj = S3Details(logger_obj)
        assert isinstance(self.obj, S3Details)

    def test_upload_s3_passed(self, s3_mock, logger_obj, file, key):
        """This tests for put object in S3 class S3details"""
        self.s3_client = S3Details(logger_obj)
        res = self.s3_client.upload_file(file, key)
        assert res == file

    @pytest.mark.xfail
    def test_upload_s3_failed(self, logger_obj, no_file, key):
        """This method will test for the report is uploaded to S3 in class S3Operations"""
        self.s3_client = S3Details(logger_obj)
        res = self.s3_client.upload_file(no_file, key)
        assert res is None


class Test_NobelprizeLaureatesFromApi:
    """This class tests for all the possible testcases in NobelprizeLaureatesFromApi class
    for getting nobelprizes and laureates from api"""

    def test_nobelprize_laureates_api_object(self, logger_obj):
        """This method tests the instance of class NobelprizeLaureatesFromApi"""
        self.api_obj = NobelprizeLaureatesFromApi(logger_obj)
        assert isinstance(self.api_obj, NobelprizeLaureatesFromApi)

    def test_pull_nobelprizes_from_api_isdone(
        self, logger_obj, base_url, endpoint_prize, start_year
    ):
        """This method tests for pulling nobel prize details from api"""
        self.api_obj = NobelprizeLaureatesFromApi(logger_obj)
        response = self.api_obj.pull_nobelprizes_laureates_from_api(endpoint_prize, start_year)
        assert response is not None

    @pytest.mark.xfail
    def test_pull_nobelprizes_from_api_notdone(
        self, logger_obj, base_url, dummy_endpoint, year_none
    ):
        """This method tests for pulling nobel prize details from api"""
        self.api_obj = NobelprizeLaureatesFromApi(logger_obj)
        response = self.api_obj.pull_nobelprizes_laureates_from_api(dummy_endpoint, year_none)
        assert response is None

    def test_pull_laureates_from_api_isdone(
        self, logger_obj, base_url, endpoint_laureate, start_year
    ):
        """This method tests for pulling nobel prize details from api"""
        self.api_obj = NobelprizeLaureatesFromApi(logger_obj)
        response = self.api_obj.pull_nobelprizes_laureates_from_api(endpoint_laureate, start_year)
        assert response is not None

    @pytest.mark.xfail
    def test_pull_laureates_from_api_notdone(self, logger_obj, base_url, dummy_endpoint, year_none):
        """This method tests for pulling nobel prize details from api"""
        self.api_obj = NobelprizeLaureatesFromApi(logger_obj)
        response = self.api_obj.pull_nobelprizes_laureates_from_api(dummy_endpoint, year_none)
        assert response is None


class Test_NobelPrizeLaureatesPartitionS3:
    """This class has methods to test possible testcases in NobelPrizeLaureatesPartitionS3
    class for nobelprize and laureates getting response and partition"""

    def test_nobelprize_laureates_parition_object(
        self, logger_obj, start_year, end_year, endpoint_prize
    ):
        """This method tests the instance of class NobelPrizeLaureatesPartitionS3"""
        self.obj = NobelPrizeLaureatesPartitionS3(logger_obj, start_year, end_year, endpoint_prize)
        assert isinstance(self.obj, NobelPrizeLaureatesPartitionS3)

    def test_fetch_nobelprize_laureate_from_api_each_year_done(
        self, logger_obj, start_year, end_year, endpoint_prize
    ):
        """This method tests for fetching the nobel prize and laureates details from the api
        for each year between the given years by making them as a list"""
        self.obj = NobelPrizeLaureatesPartitionS3(logger_obj, start_year, end_year, endpoint_prize)
        years_list = self.obj.fetch_nobelprize_laureate_from_api_each_year()
        assert isinstance(years_list, list)

    @pytest.mark.xfail
    def test_fetch_nobelprize_laureate_from_api_each_year_notdone(
        self, logger_obj, year_none, end_year, endpoint_prize
    ):
        """This method tests failed in fetching the nobel prize and laureates details from the api
        for each year between the given years by making them as a list"""
        self.obj = NobelPrizeLaureatesPartitionS3(logger_obj, year_none, end_year, endpoint_prize)
        years_list = self.obj.fetch_nobelprize_laureate_from_api_each_year()
        assert years_list is None

    def test_fetch_nobelprize_laureate_from_api_each_year_withno_endyear_done(
        self, logger_obj, start_year, endpoint_prize
    ):
        """This method tests for fetching the nobel prize and laureates details from the api
        for each year between the given years by making them as a list"""
        self.obj = NobelPrizeLaureatesPartitionS3(logger_obj, start_year, None, endpoint_prize)
        years_list = self.obj.fetch_nobelprize_laureate_from_api_each_year()
        assert isinstance(years_list, list)

    @pytest.mark.xfail
    def test_fetch_nobelprize_laureate_from_api_each_year_withno_endyear_notdone(
        self, logger_obj, start_year, year_none, endpoint_prize
    ):
        """This method tests failed in fetching the nobel prize and laureates details from the api
        for each year between the given years by making them as a list"""
        self.obj = NobelPrizeLaureatesPartitionS3(logger_obj, start_year, year_none, endpoint_prize)
        years_list = self.obj.fetch_nobelprize_laureate_from_api_each_year()
        assert years_list == [start_year]

    def test_get_nobelprize_response_from_api_isdone(
        self, logger_obj, start_year, end_year, endpoint_prize, year_list
    ):
        """This method tests for geting the nobelprizes response from api
        and convert to a dataframe using pandas"""
        self.obj = NobelPrizeLaureatesPartitionS3(logger_obj, start_year, end_year, endpoint_prize)
        data = self.obj.get_response_from_api(year_list)
        assert isinstance(data, pd.DataFrame)

    @pytest.mark.xfail
    def test_get_nobelprize_response_from_api_notdone(
        self, logger_obj, year_none, end_year, dummy_endpoint, year_list
    ):
        """This method tests for geting the nobelprizes response from api
        and convert to a dataframe using pandas"""
        self.obj = NobelPrizeLaureatesPartitionS3(logger_obj, year_none, end_year, dummy_endpoint)
        data = self.obj.get_response_from_api(year_list)
        assert data is None

    def test_get_laureate_response_from_api_isdone(
        self, logger_obj, start_year, end_year, endpoint_laureate, year_list
    ):
        """This method tests for geting the laureates response from api
        and convert to a dataframe using pandas"""
        self.obj = NobelPrizeLaureatesPartitionS3(
            logger_obj, start_year, end_year, endpoint_laureate
        )
        data = self.obj.get_response_from_api(year_list)
        assert isinstance(data, pd.DataFrame)

    @pytest.mark.xfail
    def test_get_laureate_response_from_api_notdone(
        self, logger_obj, year_none, end_year, dummy_endpoint, year_list
    ):
        """This method tests for not geting the  laureates response from api
        and convert to a dataframe using pandas"""
        self.obj = NobelPrizeLaureatesPartitionS3(logger_obj, year_none, end_year, dummy_endpoint)
        data = self.obj.get_response_from_api(year_list)
        assert data is None

    def test_put_partition_path_for_nobelprize_isdone(
        self, logger_obj, start_year, end_year, endpoint_prize, partition_for_prize
    ):
        """This method tests for giving the partition path for nobelprize endpoint based on year"""
        self.obj = NobelPrizeLaureatesPartitionS3(logger_obj, start_year, end_year, endpoint_prize)
        path = self.obj.put_partition_path(start_year)
        assert path == partition_for_prize

    @pytest.mark.xfail
    def test_put_partition_path_for_nobelprize_is_notdone(
        self, logger_obj, year_none, end_year, endpoint_prize, wrong_partition_prize
    ):
        """This method tests failure case for giving the partition path
        based on year for nobelprize endpoint"""
        self.obj = NobelPrizeLaureatesPartitionS3(logger_obj, year_none, end_year, endpoint_prize)
        response = self.obj.put_partition_path(year_none)
        assert response == wrong_partition_prize

    def test_put_partition_path_for_laureates_isdone(
        self, logger_obj, start_year, end_year, endpoint_laureate, partition_for_laureates
    ):
        """This method tests for giving the partition path for nobelprize endpoint based on year"""
        self.obj = NobelPrizeLaureatesPartitionS3(
            logger_obj, start_year, end_year, endpoint_laureate
        )
        path = self.obj.put_partition_path(start_year)
        assert path == partition_for_laureates

    @pytest.mark.xfail
    def test_put_partition_path_for_laureates_is_notdone(
        self, logger_obj, year_none, end_year, endpoint_laureate, wrong_partition_laureate
    ):
        """This method tests failure case for giving the partition path
        based on year for nobelprize endpoint"""
        self.obj = NobelPrizeLaureatesPartitionS3(
            logger_obj, year_none, end_year, endpoint_laureate
        )
        response = self.obj.put_partition_path(year_none)
        assert response == wrong_partition_laureate

    def test_create_json_file_partition_isdone(
        self, logger_obj, start_year, end_year, endpoint_laureate, dataframe, file_name
    ):
        """This method tests for the creation of json file is done"""
        self.obj = NobelPrizeLaureatesPartitionS3(
            logger_obj, start_year, end_year, endpoint_laureate
        )
        file = self.obj.create_json_file_partition(dataframe, start_year)
        assert file == file_name

    @pytest.mark.xfail
    def test_create_json_file_partition_notdone(
        self, logger_obj, year_none, end_year, endpoint_laureate, empty_dataframe, wrong_file_name
    ):
        """This method tests for the creation of json file is not done"""
        self.obj = NobelPrizeLaureatesPartitionS3(
            logger_obj, year_none, end_year, endpoint_laureate
        )
        file = self.obj.create_json_file_partition(empty_dataframe, year_none)
        assert file == wrong_file_name
