"""This module has script for testing mocking of AWS services, 
tests the python script of changing file in sql format"""
import os
import pytest
import boto3
from moto import mock_athena, mock_glue, mock_s3
from check import *


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


# @pytest.fixture
@mock_glue
def test_database_tables(aws_credentials):
    """This method tests the create database in glue from mocked AWS services"""
    glue_client = boto3.client("glue", region_name="us-east-1")
    response = glue_client.create_database(
        DatabaseInput={
            "Name": "SampleTestDatabase",
            "Description": "Sample database created for testing",
        }
    )
    response_data = glue_client.get_databases()
    table = glue_client.create_table(
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
    response_table = glue_client.get_tables(DatabaseName="SampleTestDatabase")
    list = [response_table["TableList"]]


@pytest.fixture
def bucket_name():
    """This method returns the bucket name"""
    return "test_bucket"


@mock_s3
def test_create_bucket(bucket_name):
    """This method tests the s3 operations"""
    S3 = boto3.client("s3", region_name="us-east-1")
    S3.create_bucket(Bucket=bucket_name)

    result = S3.list_buckets()
    S3.put_object(Bucket=bucket_name, Body="flow.txt", Key="flow.txt")
    obj = S3.list_objects(Bucket=bucket_name)
    assert len(result["Buckets"]) == 1
    assert result["Buckets"][0]["Name"] == "test_bucket"
    assert obj["Contents"][0]["Key"] == "flow.txt"


@mock_athena
def test_start_query_execution(aws_credentials):
    """This method tests the start query execution of athena"""
    client = boto3.client("athena", region_name="us-east-1")
    bucket = "test_bucket"
    database = "database1"
    table = "table1"
    sec_response = store_sql_query_s3(client, bucket, database, table)
    assert "QueryExecutionId" in sec_response


def change_sql_undertest():
    """Checks for the python script build for changing sql name"""
    s3_client2 = boto3.resource("s3")
    database_name = "database1"
    table_name = "table1"
    bucket_name = "test_bucket"
    response = change_file_sqlt(s3_client2, bucket_name, database_name, table_name)
    # return obj["Body"].read().decode("utf-8")
    return response


def test_function2(aws_credentials):
    """This is a test method for calling function"""
    assert change_sql_undertest() == "success"
