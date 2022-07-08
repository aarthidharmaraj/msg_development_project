"""This module is to create a table in dynamodb, put an item, update and get an item from the table"""
import boto3
from moto import mock_dynamodb
class DynamoDB:
    """This class methods to cretae table, put ,get and item in table and update the item in the table"""
    
    def __init__(self,logger):
        """This is the init method for the class DynamoDB"""
        self.logger=logger
        with mock_dynamodb():
            self.resource = boto3.resource('dynamodb', 'us-east-1')
    
    def create_table_dynamodb(self,kwargs):
        """This method will create a table with the arguments in dynamoDB"""
        try:
            with mock_dynamodb():
                dynamodb = boto3.resource('dynamodb', 'us-east-1')
                table = dynamodb.create_table(**kwargs)
                print(table)
        except Exception as err:
            self.logger.error("Cannot create the table in dynamoDB %s",err)
            print(err)
            table=None
        return table
    
    def put_item_table(self,table_name,table_kwargs,put_kwargs):
        """This method put an item in a table"""
        try:
            with mock_dynamodb():
                dynamodb = boto3.resource('dynamodb', 'us-east-1')
                dynamodb.create_table(**table_kwargs)
                table =dynamodb.Table(table_name)
                item = table.put_item(**put_kwargs)
            print(table)
        except Exception as err:
            print(err)
            self.logger.error("Cannot put an item in the table %s",err)
            item=None
        return item
    
    def get_item_from_table(self,table_name,table_kwargs,put_kwargs,get_kwargs):
        """This method gets an item from the table"""
        try:
            with mock_dynamodb():
                dynamodb = boto3.resource('dynamodb', 'us-east-1')
                dynamodb.create_table(**table_kwargs)
                table =dynamodb.Table(table_name)
                item = table.put_item(**put_kwargs)
                item = table.get_item(**get_kwargs)
                print(item['Item']['section_data'])
        except Exception as err:
            self.logger.error("Cannot get an item from the table %s",err)
            item=None
            print(err)
        return item['Item']['section_data']
        
    def update_item_from_table(self,table_name,kwargs):
        """This method updates an item in the table"""
        try:
            table = self.resource.Table(table_name)
            item= table.update_item(**kwargs)
            print(item)
        except Exception as err:
            print(err)
            self.logger.error("Cannot update item in the table %s",err)
            item=None
        return item
    