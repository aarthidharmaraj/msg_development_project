"""This module is to mock aws dynamodb and check the methods of dynamodb"""
# import unittest
from logging.handlers import TimedRotatingFileHandler
import logging
import configparser
import os
import boto3
import pytest
from moto import mock_dynamodb
from dynamodb_for_ini import DynamodbConfigSections
from dynamodb_table_item import DynamoDB

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
        "dynamodb_test",
    )
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_file = os.path.join(log_dir, "holiday_api_test.log")
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
def dynamodb_mock():
    """This method mocks the aws dynamodb service"""
    with mock_dynamodb():
        dynamodb = boto3.resource('dynamodb', 'us-east-1')
        yield dynamodb

@pytest.fixture
def table_name():
    """This method returns the table name for dynamodb"""
    return "config_section"

@pytest.fixture
def mock_create_table(dynamodb_mock,table_name):
    """This method creates the table with the mocked dynamodb"""
    table = dynamodb_mock.create_table(
        TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'section_id',
                    'KeyType': 'HASH'
                },
                {
                'AttributeName': 'section_name',
                'KeyType': 'RANGE'    #Sort Key
                }

            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'section_id',
                    'AttributeType': 'N'
                },
                {
                'AttributeName': 'section_name',
                'AttributeType': 'S'
                 },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
    yield
    
@pytest.fixture
def mock_put_item(dynamodb_mock,mock_create_table,table_name):
    """This method mocks for put an item in the table in mocked dynamodb"""
    table = dynamodb_mock.Table(table_name)
    table.put_item(
    Item={
        'section_id': 1,
        'section_name': 'freesound_api',
            's3_path':'freesound_api/source',
            'bucket': 'msg-practice-induction',
            'basic_url':'https://freesound.org/apiv2/',
            'fernet_key':'5EIHJePaBEaVlhLdk5aZT3hZ-vtDnAdSuHzfB_GRd7Q=',
            'api_key ': 'gAAAAABinfOH1Um-c_8wlKMfL46pVyer0vPQtM872VoFPilA1KhoEI6mzyUdpGEp4HbjKj7aZcxCMIL8PpH_PAz2m2eXVjWXBlueVHmkjj3OTlq9dE8Y9BygupzIcB5WxpmJpVUgGucU'}
    )
    yield
    
@pytest.fixture
def mock_update_item(dynamodb_mock,mock_cretae_table,table_name,mock_put_item):
    """This method mocks for update item in table in mocked dynamodb"""
    table = dynamodb_mock.Table(table_name)
    resp = table.update_item(
    Key={'section_id': 1},
    UpdateExpression="SET s3_path= :s",
    ExpressionAttributeValues={':s': 'source/example'},
    ReturnValues="UPDATED_NEW"
    )
    print(resp['Attributes'])
    return resp['Attributes']

@pytest.fixture
def table_args(table_name):
    """This method returns the arguments for create table method in dynamodb"""
    parameters={'TableName':table_name,'KeySchema':[{'AttributeName': 'section_id','KeyType': 'HASH'},{'AttributeName': 'section_name','KeyType': 'RANGE'}], 'AttributeDefinitions':[{'AttributeName': 'section_id','AttributeType': 'N'},{'AttributeName': 'section_name','AttributeType': 'S' }],'ProvisionedThroughput':{'ReadCapacityUnits': 5,'WriteCapacityUnits': 5},}
    return parameters

@pytest.fixture
def false_table_args(table_name):
    """This method returns the false arguments for create table method in dynamodb"""
    parameters={'KeySchema':[{'AttributeName': 'section_id','KeyType': 'HASH'},{'AttributeName': 'section_name','KeyType': 'RANGE'}], 'AttributeDefinitions':[{'AttributeName': 'section_id','AttributeType': 'N'},{'AttributeName': 'section_name','AttributeType': 'S' }],'ProvisionedThroughput':{'ReadCapacityUnits': 5,'WriteCapacityUnits': 5},}
    return parameters

@pytest.fixture
def put_item_args():
    """This method returns the arguments for put item method in dynamodb"""
    parameters={'Item':{
        'section_id': 1,
        'section_name': 'freesound_api',
        'section':{
            's3_path':'freesound_api/source',
            'bucket': 'msg-practice-induction',
            'basic_url':'https://freesound.org/apiv2/',
            'fernet_key':'5EIHJePaBEaVlhLdk5aZT3hZ-vtDnAdSuHzfB_GRd7Q=',
            'api_key ': 'gAAAAABinfOH1Um-c_8wlKMfL46pVyer0vPQtM872VoFPilA1KhoEI6mzyUdpGEp4HbjKj7aZcxCMIL8PpH_PAz2m2eXVjWXBlueVHmkjj3OTlq9dE8Y9BygupzIcB5WxpmJpVUgGucU'}
    },}
    return parameters
    
@pytest.fixture
def false_put_item_args():
    """This method returns the false arguments for put item method in dynamodb"""
    parameters={
        'section_id': 1,
        'section_name': 'freesound_api',
    }
    return parameters
    
@pytest.fixture
def get_item_args():
    """This method returns the arguments for get item method in dynamodb"""
    parameters={'Key':{
        'section_id': 1,
        'section_name':'freesound_api'
    },
    }
    return parameters

@pytest.fixture
def false_get_item_args():
    """This method returns the false arguments for get item method in dynamodb"""
    parameters={
        'section_id': 3,
        'section_name':'sample_api'
    }
    return parameters

@pytest.fixture
def update_item_args():
    """This method returns the arguments for updating item in dynamodb"""
    parameters={'Key':{'section_id': 1,'section_name':'freesound_api'},'UpdateExpression':"SET s3_path= :s",'ExpressionAttributeValues':{':s': 'source/example'},'ReturnValues':"UPDATED_NEW"}
    return parameters

@pytest.fixture
def false_update_item_args():
    """This method returns the false arguments for updating item in dynamodb"""
    parameters={'ITEM':{'section_id': 3,'section_name':'api'},'UpdateExpression':"SET s3_path= :s",'ExpressionAttributeValues':{':s': 'source/example'},'ReturnValues':"UPDATED_NEW"}
    return parameters

class TestDynamo():
    """This class has methods to mock dynamodb and test the methods"""

    def test_dynamodb_object(self):
        """This method tests for the instance of the class DynamodbConfigSections"""
        self.dynamodb=DynamodbConfigSections()
        assert isinstance (self.dynamodb,DynamodbConfigSections)
        
    def test_create_table_dynamodb_done(self,dynamodb_mock,table_args):
        """This method tests for the creation of table with the given parameters in dynamodb create table method """
        self.dynamodb=DynamodbConfigSections()
        table=self.dynamodb.create_table(table_args)
        assert table is not None
        assert table == dynamodb_mock.Table(name='config_section')
    
    @pytest.mark.xfail
    def test_create_table_dynamodb_notdone(self,dynamodb_mock,false_table_args):
        """This method tests for the creation of table with the given parameters in dynamodb create table method is not done"""
        self.dynamodb=DynamodbConfigSections()
        table=self.dynamodb.create_table(false_table_args)
        assert table is None
        
        
    def test_put_item_dynamodb_done(self,put_item_args,mock_create_table,table_name):
        """This method tests for the Insertion of item in table with the given parameters in dynamodb put item method"""
        self.dynamodb=DynamodbConfigSections()
        item=self.dynamodb.put_item_in_table_dynamodb(table_name,put_item_args)
        # item={'ResponseMetadata': {'RequestId': 'SGB9EBUNXUEU0N341ZNWEVLCLYF0WGFCIUSWVODI9XUNGI8ARIFH', 'HTTPStatusCode': 200, 'HTTPHeaders': {'server': 'amazon.com', 'x-amzn-requestid': 'SGB9EBUNXUEU0N341ZNWEVLCLYF0WGFCIUSWVODI9XUNGI8ARIFH', 'x-amz-crc32': '2745614147'}, 'RetryAttempts': 0}}
        assert item is not None
        assert item['ResponseMetadata']['HTTPStatusCode'] ==200
       
    @pytest.mark.xfail 
    def test_put_item_dynamodb_notdone(self,false_put_item_args,mock_create_table,table_name):
        """This method tests for the Insertion of item in table with the given parameters in dynamodb put item method"""
        self.dynamodb=DynamodbConfigSections()
        item=self.dynamodb.put_item_in_table_dynamodb(table_name,false_put_item_args)
        assert item is None
        
    def test_get_item_dynamodb_done(self,get_item_args,mock_create_table,table_name,mock_put_item):
        """This method tests for getting an item and its data from table with the given parameters in dynamodb get item method is done"""
        self.dynamodb=DynamodbConfigSections()
        item=self.dynamodb.get_item_from_table_dynamodb(table_name,get_item_args)
        # {'Item': {'section': {'api_key ': 'gAAAAABinfOH1Um-c_8wlKMfL46pVyer0vPQtM872VoFPilA1KhoEI6mzyUdpGEp4HbjKj7aZcxCMIL8PpH_PAz2m2eXVjWXBlueVHmkjj3OTlq9dE8Y9BygupzIcB5WxpmJpVUgGucU', 'basic_url': 'https://freesound.org/apiv2/', 'bucket': 'msg-practice-induction', 'fernet_key': '5EIHJePaBEaVlhLdk5aZT3hZ-vtDnAdSuHzfB_GRd7Q=', ...}, 'section_id': Decimal('1'), 'section_name': 'freesound_api'}, 'ResponseMetadata': {'HTTPHeaders': {'server': 'amazon.com', 'x-amz-crc32': '4223262621', 'x-amzn-requestid': '13IMJBQYXNXF50ZU6P95IFC0TNCH1I24B1E4T34NGR2VX0HGMLJL'}, 'HTTPStatusCode': 200, 'RequestId': '13IMJBQYXNXF50ZU6P95IFC0TNCH1I24B1E4T34NGR2VX0HGMLJL', 'RetryAttempts': 0}}
        assert item is not None
        # assert item['Item']['section'].get('bucket') ==  'msg-practice-induction'
        assert item['Item'].get('bucket') ==  'msg-practice-induction'
        assert item['Item'].get('basic_url') ==  'https://freesound.org/apiv2/'
        
    @pytest.mark.xfail
    def test_get_item_dynamodb_not_done(self,false_get_item_args,mock_create_table,table_name,mock_put_item):
        """This method tests for getting an item and its data from table with the given parameters in dynamodb get item method is not done"""
        self.dynamodb=DynamodbConfigSections()
        item=self.dynamodb.get_item_from_table_dynamodb(table_name,false_get_item_args)
        assert item is None
        
    def test_update_item_dynamodb_done(self,mock_create_table,mock_put_item,table_name,update_item_args,get_item_args):
        """This method tests for updating an item in table with the given parameters in dynamodb update item method is done """
        self.dynamodb=DynamodbConfigSections()
        update_item=self.dynamodb.update_item_in_table_dynamodb(table_name,update_item_args)
        # item={'Attributes': {'s3_path': 'source/example'}, 'ConsumedCapacity': {'TableName': 'config_section', 'CapacityUnits': 0.5}, 'ResponseMetadata': {'RequestId': 'ZJU128PT3M48ZBKVORZAYNGDOIN06CFFN77F36XB56YGXUWYHU03', 'HTTPStatusCode': 200, 'HTTPHeaders': {'server': 'amazon.com', 'x-amzn-requestid': 'ZJU128PT3M48ZBKVORZAYNGDOIN06CFFN77F36XB56YGXUWYHU03', 'x-amz-crc32': '3629707218'}, 'RetryAttempts': 0}}
        assert update_item is not None
        get_item=self.dynamodb.get_item_from_table_dynamodb(table_name,get_item_args)
        assert get_item['Item']['s3_path']=='source/example'
        
    @pytest.mark.xfail
    def test_update_item_dynamodb_not_done(self,mock_create_table,mock_put_item,table_name,false_update_item_args,get_item_args):
        """This method tests for updating an item in table with the given parameters in dynamodb update item method is not done """
        self.dynamodb=DynamodbConfigSections()
        update_item=self.dynamodb.update_item_in_table_dynamodb(table_name,false_update_item_args)
        assert update_item is None
        get_item=self.dynamodb.get_item_from_table_dynamodb(table_name,get_item_args)
        assert get_item['Item']['s3_path']=='freesound_api/source'
        
        
    
        