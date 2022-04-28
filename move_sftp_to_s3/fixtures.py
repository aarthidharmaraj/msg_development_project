""" This module creates a pytest fixture for sftp and S3"""
import pytest
from moto import mock_s3
import boto3


@pytest.fixture
def mock_sftp(sftp_server):
    """This is the fixture for mocking sftp server"""
    print(sftp_server.host)
    with sftp_server.client("test_user1") as client:
        sftp = client.open_sftp()
        yield sftp


@pytest.fixture(scope="function")
def s3_mock():
    """This method creates a mock for s3 services"""
    with mock_s3():
        yield boto3.client("s3", region_name="us-east-1")
