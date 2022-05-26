"""This module has testcases for methods in s3, sftp modules and upload_local, 
load_sftp_to_s3 module"""
from logging.handlers import TimedRotatingFileHandler
import logging
import configparser
import os
from zipfile import ZipFile
import pytest
import boto3
import pysftp
from mock import patch
from moto import mock_s3
from aws_s3.s3_details import S3Details
from sftp.sftp_connection import SftpConnection
from moveasp_file_from_sftp_to_s3 import MoveAspSftpToS3
from sftp_to_s3_upload_local import MoveSftpToS3Local
from contextlib import closing
import py.path


@pytest.fixture
def root_path(sftp_server):
    return py.path.local(sftp_server.root)


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
def logger_obj(parent_dir, config_path):
    """This method returns the logger object of time rotating time logger"""
    log_dir = os.path.join(
        parent_dir,
        config_path["local"]["log_path"],
        "load_sftp_to_s3",
    )
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_file = os.path.join(log_dir, "sftp_to_s3_logfile.log")
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
def zip_source(sftp_path, file_name):
    """This method returns the zip source"""
    zip_source = sftp_path + file_name
    return zip_source


@pytest.fixture
def text_source(zip_source, upload_path, file_name):
    """This method returns the zip source"""
    with ZipFile(zip_source, "r") as zipobj:
        zipobj.extractall(upload_path)
    text_source = upload_path + "/" + file_name[0:12] + ".txt"
    return text_source


@pytest.fixture
def source_none():
    """This method returns the source as none"""
    return None


@pytest.fixture
def file_name():
    """This method returns the file name"""
    return "ASP_20220523.zip"

@pytest.fixture
def base_file(file_name):
    """This method returns the base file from file_name"""
    return file_name.split(".")[0]

@pytest.fixture
def wrong_base_file():
    """This method returns the wrong file name"""
    return "ASPIRE_202210230"


@pytest.fixture
def text_file_name():
    """This method returns the file name for text file"""
    return "ASP_20220523.txt"


@pytest.fixture
def partition_path():
    """This method returns the partition path"""
    path = "pt_year=2022/pt_month=05/pt_day=23"
    return path


@pytest.fixture
def wrong_partition_path():
    """This method returns the wrong partition path"""
    path = "pt_year=2022/pt_month=02/pt_day=.z"
    return path


@pytest.fixture
def upload_path(parent_dir, config_path):
    """This method returns the sql path"""
    local_path = os.path.join(parent_dir, config_path["local"]["data_path"], "sftp_to_s3_data_test")
    if not os.path.exists(local_path):
        os.makedirs(local_path)
    return local_path


@pytest.fixture
def duplicate_path():
    """This method returns the duplicate path for list files from sftp local"""
    return "abc/xyz/path"


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
def bucket_name(config_path):
    bucket_name = config_path["s3"]["bucket_name"]
    return bucket_name


@pytest.fixture
def bucket_path(config_path):
    bucket_path = config_path["move_asp_from_sftp_to_s3"]["s3_path_source"]
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
def source_path():
    """This method returns the source path for zip file"""
    path_source = "host-aspire/source/"
    return path_source


@pytest.fixture
def stage_path():
    """This method returns the stage path for text file"""
    path_stage = "host-aspire/stage/"
    return path_stage


@pytest.fixture
def sftp_path():
    """This method returns the sftp path"""
    sftp_path = "ASP SFTP server/D/Export/"
    return sftp_path


class TestUploadPartitionLocal:
    """This class has methods to test possible testcases in
    MoveSftpToS3Local for upload local module"""

    def test_upload_local_object(self, logger_obj, config_path):
        """This method tests the instance of class MoveSftpToS3Local"""
        self.obj_local = MoveSftpToS3Local(logger_obj, config_path)
        assert isinstance(self.obj_local, MoveSftpToS3Local)

    def test_get_latest_file_is_done(
        self, logger_obj, config_path, upload_path, source_path, sftp_path
    ):
        """This method tests for gettiing the latest file from sftp local"""
        self.obj_local = MoveSftpToS3Local(logger_obj, config_path)
        latest_file = self.obj_local.get_latest_files_from_local(
            upload_path, source_path, sftp_path
        )
        assert isinstance(latest_file, list)

    @pytest.mark.xfail
    def test_get_latest_file_is_notdone(
        self, logger_obj, config_path, upload_path, source_none, sftp_path
    ):
        """This method tests for gettiing the latest file from sftp local"""
        self.obj_local = MoveSftpToS3Local(logger_obj, config_path)
        latest_file = self.obj_local.get_latest_files_from_local(
            upload_path, source_none, sftp_path
        )
        assert latest_file is None

    def test_get_list_of_files_is_done(self, logger_obj, config_path, sftp_path):
        """This method gets the list of files from the local path is done"""
        self.obj_local = MoveSftpToS3Local(logger_obj, config_path)
        list_file = self.obj_local.list_files_sftp_local(sftp_path)
        assert isinstance(list_file, list)

    @pytest.mark.xfail
    def test_get_list_of_files_is_notdone(self, logger_obj, config_path, duplicate_path):
        """This method gets the list of files from the local path is not done"""
        self.obj_local = MoveSftpToS3Local(logger_obj, config_path)
        list_file = self.obj_local.list_files_sftp_local(duplicate_path)
        assert list_file is None

    def test_upload_zip_to_local_file_is_done(
        self, logger_obj, upload_path, config_path, zip_source, file_name, partition_path
    ):
        """This method will test for the nobelprize details is written to local file
        in class  MoveSftpToS3Local"""
        self.obj_local = MoveSftpToS3Local(logger_obj, config_path)
        local_file = self.obj_local.upload_partition_s3_local(
            upload_path, zip_source, file_name, partition_path
        )
        assert local_file == "ASP_20220523.zip"

    @pytest.mark.xfail
    def test_upload_zip_to_local_file_is_not_done(
        self,
        logger_obj,
        upload_path,
        config_path,
        source_none,
        file_name,
        partition_path,
    ):
        """This method will test for nobelprize details is not written to local file
        in class  MoveSftpToS3Local"""
        self.obj_local = MoveSftpToS3Local(logger_obj, config_path)
        local_file = self.obj_local.upload_partition_s3_local(
            upload_path, source_none, file_name, partition_path
        )
        assert local_file is None

    def test_upload_text_to_local_file_is_done(
        self,
        logger_obj,
        upload_path,
        text_source,
        text_file_name,
        config_path,
        partition_path,
    ):
        """This method will test for the laureates details is written to local file
        in class  MoveSftpToS3Local"""
        self.obj_local = MoveSftpToS3Local(logger_obj, config_path)
        local_file = self.obj_local.upload_partition_s3_local(
            upload_path, text_source, text_file_name, partition_path
        )
        assert local_file == text_file_name

    @pytest.mark.xfail
    def test_upload_text_to_local_file_is_notdone(
        self,
        logger_obj,
        upload_path,
        source_none,
        text_file_name,
        config_path,
        partition_path,
    ):
        """This method will test for the laureates details is written to local file
        in class  MoveSftpToS3Local"""
        self.obj_local = MoveSftpToS3Local(logger_obj, config_path)
        local_file = self.obj_local.upload_partition_s3_local(
            upload_path, source_none, text_file_name, partition_path
        )
        assert local_file is None
        
class Test_s3:
    """This class will test all possible testcases in s3 module"""
    
    def test_s3_object(self,logger_obj,config_path):
        """This method test the instance belong to the class of S3Service"""
        self.s3_obj = S3Details(logger_obj,config_path)
        assert isinstance(self.s3_obj, S3Details)
        
    def test_upload_file_s3_done(self,file,config_path,key,bucket_name,s3_mock_client,s3_bucket,bucket_path,logger_obj):
        """This method will test file is sucessfully uploaded"""
        self.my_client = S3Details(logger_obj,config_path)
        response = self.my_client.upload_file(file,bucket_name,bucket_path,key)
        assert response is not None
        
    @pytest.mark.xfail
    def test_upload_file_S3_notdone(self,config_path,key,no_file,bucket_name,s3_mock_client,s3_bucket,bucket_path,logger_obj):
        """This method will test file is not uploaded"""
        self.my_client = S3Details(logger_obj,config_path)
        response = self.my_client.upload_file(no_file,bucket_name,bucket_path,key)
        assert response is None
    
    def test_get_list_of_files_s3_done(self,config_path,bucket_name,s3_mock_client,s3_bucket,s3_upload,bucket_path,logger_obj):
        """this method tests for getting the list of files from s3 is done"""
        self.my_client = S3Details(logger_obj,config_path)
        response=self.my_client.get_list_of_files_in_s3(bucket_name,bucket_path)
        assert isinstance(response,list)
        
    @pytest.mark.xfail
    def test_get_list_of_files_s3_notdone(self,config_path,key,s3_mock_client,s3_bucket,s3_upload,bucket_path,logger_obj):
        """this method tests for getting the list of files from s3 is not done"""
        self.my_client = S3Details(logger_obj,config_path)
        response=self.my_client.get_list_of_files_in_s3("None",bucket_path+key)
        assert response is None


class TestLoadSftpToS3:
    """This class has methods to test possible testcases in MoveSftpToS3 class"""

    def test_load_sftp_to_s3_object(self):
        """This method tests the instance of class MoveSftpToS3 class"""
        self.obj = MoveAspSftpToS3()
        assert isinstance(self.obj, MoveAspSftpToS3)

    def test_put_partition_path_isdone(self, partition_path, base_file):
        """This method tests for giving the partition path for based on year,month and date"""
        self.obj = MoveAspSftpToS3()
        path = self.obj.put_partition_path(base_file)
        assert path == partition_path

    @pytest.mark.xfail
    def test_put_partition_path_is_notdone(self, wrong_base_file, wrong_partition_path):
        """This method tests failure case for giving the partition path
        based on year,month and date"""
        self.obj = MoveAspSftpToS3()
        response = self.obj.put_partition_path(wrong_base_file)
        assert response is None

    def test_extract_file_is_done(self, zip_source, file_name):
        """this method tests for the extraction of text file from zip is done"""
        self.obj = MoveAspSftpToS3()
        text_source = self.obj.extract_the_files_from_zip(zip_source, file_name)
        assert text_source is not None

    @pytest.mark.xfail
    def test_extract_file_is_notdone(self, source_none, file_name):
        """this method tests for the extraction of text file from zip is done"""
        self.obj = MoveAspSftpToS3()
        text_source = self.obj.extract_the_files_from_zip(source_none, file_name)
        assert text_source is None

    def test_get_zip_file_is_done(self):
        """This method tests for getting zip file from sftp local is done"""
        self.obj = MoveAspSftpToS3()
        zip_source = self.obj.get_zip_file_from_sftp()
        assert zip_source is not None


class TestSftpConnection:
    """This class has methods to test all possible testcases in sftp module"""

    def test_sftp_object(self, logger_obj, config_path):
        """This method tests the instances of sftp connection"""
        self.sftp = SftpConnection(logger_obj, config_path)
        assert isinstance(self.sftp, SftpConnection)

    def test_sftp_connection(self):
        """This method tests for the patch mock connection of sftp"""
        with patch("pysftp.Connection") as mock_connection:
            with pysftp.Connection("1.2.3.4", "user", "pwd", 12345) as sftp:
                sftp.get("filename")
            mock_connection.assert_called_with("1.2.3.4", "user", "pwd", 12345)
            sftp.get.assert_called_with("filename")


def test_sftp_list_files_passed(root_path, sftp_client):
    """This method tests for listing of files from sftp failed"""
    try:
        sftp = sftp_client.open_sftp()
        assert sftp.listdir(".") == []
        root_path.join("sample_file.zip").write("sample zip file")
        root_path.join("subdir/file2.zip").write("sample zip file with directory", ensure=True)
        with closing(sftp.open("sample_file.zip", "r")) as data:
            assert data.read() == b"sample zip file"
    except Exception:
        sftp = None
    assert (sftp.listdir(".")) == ["sample_file.zip", "subdir"]
    assert sftp.listdir("subdir") == ["file2.zip"]


@pytest.mark.xfail
def test_sftp_list_files_failed(duplicate_path, sftp_client):
    """This method tests for listing of files from sftp failed"""
    try:
        sftp = sftp_client.open_sftp()
        duplicate_path.join("sample_file.zip").write("sample zip file")
        duplicate_path.join("subdir/file2.zip").write("sample zip file with directory", ensure=True)
        with closing(sftp.open("sample_file.zip", "r")) as data:
            assert data.read() == b"sample zip file"
    except Exception:
        sftp = None
    assert sftp is None
