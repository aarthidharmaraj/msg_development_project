"""This module has test scripts for glue and S3 
without repition use of client details"""
import os
import pytest
import boto3
from moto import mock_glue, mock_s3


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def glue(aws_credentials):
    """This method creates a mock for glue services"""
    with mock_glue():
        yield boto3.client("glue", region_name="us-east-1")


@pytest.fixture(scope="function")
def S3(aws_credentials):
    """This method creates a mock for s3 services"""
    with mock_s3():
        yield boto3.client("s3", region_name="us-east-1")


def test_database_tables(glue):
    """This method tests the create database in glue from mocked AWS services"""
    response = glue.create_database(
        DatabaseInput={
            "Name": "SampleTestDatabase",
            "Description": "Sample database created for testing",
        }
    )
    response_data = glue.get_databases()
    assert response_data["DatabaseList"][0]["Name"] == "SampleTestDatabase"
    table = glue.create_table(
        DatabaseName="SampleTestDatabase",
        TableInput={
            "Name": "table1",
            "StorageDescriptor": {
                "Columns": [{"Name": "column1"}],
                "SortColumns": [{"Column": "column1", "SortOrder": 123}],
            },
            "PartitionKeys": [{"Name": "column1"}],
        },
        PartitionIndexes=[
            {
                "Keys": [
                    "key1",
                ],
                "IndexName": "index1",
            }
        ],
    )
    response_table = glue.get_tables(DatabaseName="SampleTestDatabase")
    assert response_table["TableList"][0]["Name"] == "table1"


@pytest.fixture
def bucket_name():
    """This method returns the bucket name"""
    return "test_bucket"


def test_create_bucket(S3, bucket_name):
    """This method tests the s3 operations"""
    S3.create_bucket(Bucket=bucket_name)

    result = S3.list_buckets()
    S3.put_object(Bucket=bucket_name, Body="flow.txt", Key="flow.txt")
    obj = S3.list_objects(Bucket=bucket_name)
    assert len(result["Buckets"]) == 1
    assert result["Buckets"][0]["Name"] == "test_bucket"
    assert obj["Contents"][0]["Key"] == "flow.txt"

@pytest.mark.xfail
def test_create_bucket_failure(S3,bucket_name):
    '''This method tests the failure cases on s3 operations'''
    S3.create_bucket(Bucket="test_bucket")

    result = S3.list_buckets()
    S3.put_object(Bucket=bucket_name,Key='database1/table1/flow.txt')
    assert len(result['Buckets']) == 1
    assert result['Buckets'][0]['Name'] == 'bucket'
