"""This module is to create a table in dynamodb, put an item, update and get an item from the table"""
import boto3
class DynamoDB:
    """This class methods to cretae table, put ,get and item in table and update the item in the table"""
    
    def __init__(self,logger):
        """This is the init method for the class DynamoDB"""
        self.logger=logger
        self.resource= boto3.resource('dynamodb')
    
    def create_table_dynamodb(self,table_name):
        """This method will create a table with the given table name in dynamoDB"""
        try:
            table = self.resource.create_table()
        except Exception as err:
            self.logger.error("Cannot create the table in dynamoDB %s",err)
            table=None
        return table
    
    def put_item_table(self,table_name):
        """This method put an item in a table"""
        try:
            table = self.resource.Table(table_name)
            item = table.put_item()
        except Exception as err:
            self.logger.error("Cannot put an item in the table %s",err)
            item=None
        return item
    
    def get_item_from_table(self,table_name):
        """This method gets an item from the table"""
        try:
            table = self.resource.Table(table_name)
            item = table.get_item()
        except Exception as err:
            self.logger.error("Cannot get an item from the table %s",err)
            item=None
        return item
        
    def update_item_from_table(self,table_name):
        """This method updates an item in the table"""
        try:
            table = self.resource.Table(table_name)
            item= table.update_item()
        except Exception as err:
            self.logger.error("Cannot update item in the table %s",err)
            item=None
        return item