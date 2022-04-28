"""This module tests the python script of moving file from Sftp to S3 with Mocked AWS services """
import os
import pytest
from mock import patch
import pysftp
from sftp.sftp_connection import SftpConnection
from aws_s3.s3_details import S3Details
from move_sftp_to_s3 import MoveSftpToS3


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


class TestS3:
    """This Test Class will check for all the possible Test cases in S3details"""

    def test_s3_objects(self):
        """This Method will test for the instance belong to the class S3details"""
        self.obj = S3Details()
        assert isinstance(self.obj, S3Details)

    def test_upload_s3_passed(self):
        """This tests for put object in S3 class S3details"""
        self.s3_client = S3Details()
        res = self.s3_client.upload_file(b"flow of code", "flow.txt")
        assert res == "success"

    @pytest.mark.xfail
    def test_upload_s3_failed(self):
        """This method will test for the report is uploaded to S3 in class S3Operations"""
        self.s3_client = S3Details()
        res = self.s3_client.upload_file(b"flow of code", "flow.txt")
        assert res=='failure'


class TestSftp:
    """This Test Class will check for all the possible Test cases in sftp_connection module"""
    def test_sftp_connection(self):
        """This method tests for the patch mock connection of sftp"""
        with patch("pysftp.Connection") as mock_connection:
            with pysftp.Connection("1.2.3.4", "user", "pwd", 12345) as sftp:
                sftp.get("filename")
            mock_connection.assert_called_with("1.2.3.4", "user", "pwd", 12345)
            # sftp.get.assert_called_with("filename")

    def test_list_files_passed(self):
        """This tests for files returning from list_files method in sftp_to_s3"""
        self.sftp = SftpConnection()
        file_list = self.sftp.list_files()
        assert isinstance(file_list,list)

    @pytest.mark.xfail
    def test_list_files_failed(self):
        """This tests for files not returning from list_files method in sftp_to_s3"""
        self.sftp = SftpConnection()
        result = self.sftp.list_files()
        assert isinstance(result,dict)



    def test_rename_file_passed(self):
        """This method will test the if the name changed or not"""
        self.sftp = SftpConnection()
        new_name = self.sftp.rename_file("file name")
        assert new_name == "success"

    @pytest.mark.xfail
    def test_rename_file_failed(self):
        """This method will test fail case that if the name changed or not"""
        self.sftp = SftpConnection()
        # self.s3 = S3Details()
        new_name = self.sftp.rename_file("different file name")
        assert new_name == "failure"


class TestMove:
    """This Test Class will check for all the possible Test cases in move_sftp_to_s3 module"""
    def test_move_file_sftp_to_s3_objects(self):
        """This tests for the instance belong to the class MoveSftpToS3"""
        self.obj_move = MoveSftpToS3()
        assert isinstance(self.obj_move, MoveSftpToS3)

    def test_move_file_is_completed(self):
        """This method will test weather the files moved sucessfully"""
        self.obj_move = MoveSftpToS3()
        result = self.obj_move.move_file_to_s3()
        assert result == "success"

    @pytest.mark.xfail
    def test_move_file_failed(self):
        """This method will test weather move file failed"""
        self.obj_mov = MoveSftpToS3()
        result = self.obj_mov.move_file_to_s3()
        assert result == "failure"
