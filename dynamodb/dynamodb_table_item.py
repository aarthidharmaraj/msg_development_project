"""This module is to create a table in dynamodb, put an item, update and get an item from the table"""
import boto3
class DynamoDB:
    """This class methods to cretae table, put ,get and item in table and update the item in the table"""
    
    def __init__(self,logger):
        """This is the init method for the class DynamoDB"""
        self.logger=logger
        self.resource= boto3.resource('dynamodb')
    
    def create_table_dynamodb(self,**kwargs):
        """This method will create a table with the arguments in dynamoDB"""
        try:
            table = self.resource.create_table(**kwargs)
        except Exception as err:
            self.logger.error("Cannot create the table in dynamoDB %s",err)
            table=None
        return table
    
    def put_item_table(self,table_name,**kwargs):
        """This method put an item in a table"""
        try:
            table = self.resource.Table(table_name)
            item = table.put_item(**kwargs)
        except Exception as err:
            self.logger.error("Cannot put an item in the table %s",err)
            item=None
        return item
    
    def get_item_from_table(self,table_name,**kwargs):
        """This method gets an item from the table"""
        try:
            table = self.resource.Table(table_name)
            item = table.get_item(**kwargs)
        except Exception as err:
            self.logger.error("Cannot get an item from the table %s",err)
            item=None
        return item
        
    def update_item_from_table(self,table_name,**kwargs):
        """This method updates an item in the table"""
        try:
            table = self.resource.Table(table_name,**kwargs)
            item= table.update_item(**kwargs)
        except Exception as err:
            self.logger.error("Cannot update item in the table %s",err)
            item=None
        return item