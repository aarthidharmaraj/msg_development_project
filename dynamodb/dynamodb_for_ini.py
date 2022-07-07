"""This module gets the arguments from the user using argparser for create table, put an
item in the table, get item from table and update an item in table in dynamodb for storing
the config values and use them in script"""
import argparse
import ast
from dynamodb_table_item import DynamoDB
from logger_path.logger_object_path import LoggerPath
datas=LoggerPath.logger_object("dynamodb")

class DynamodbConfigSections:
    """This class has methods to create table, update item,get item and put item for 
    storing config datas in dynamodb"""
    
    def __init__(self):
        """This is the init method for the class DynamodbConfigSections"""
        self.logger=datas["logger"]
        self.dynamodb=DynamoDB(self.logger)
        
    def create_table(self,args):
        """This method creates the table in dynamodb with the given parameters of config datas"""
        try:
            table=self.dynamodb.create_table_dynamodb(args)
            self.logger.info("Created the table in dynamodb for the given parameters")
        except Exception as err:
            self.logger.error("Cannot create the table in dynamodb for the given parameters")
            table=None
        return table
    
    def put_item_in_table_dynamodb(self,args):
        """This method puts an item in the table with the given parameters of config datas"""
        try:
            response=self.dynamodb.put_item_table(args)
            self.logger.info("Inserted an item in table with the given parameters")
        except Exception as err:
            self.logger.error("Cannot insert the item in the table with the given parameters")
            response=None
        return response
    
    def get_item_from_table_dynamodb(self,args):
        """This method gets an item in the table with the given parameters of config datas"""
        try:
            response=self.dynamodb.get_item_from_table(args)
            self.logger.info("Got an item in table with the given parameters")
        except Exception as err:
            self.logger.error("Cannot get the item in the table with the given parameters")
            response=None
        return response
            
    def update_item_in_table_dynamodb(self,args):
        """This method Updates an item in the table with the given parameters of config datas"""
        try:
            response=self.dynamodb.update_item_from_table(args)
            self.logger.info("Updated the item in table with the given parameters")
        except Exception as err:
            self.logger.error("Cannot update the item in the table with the given parameters")
            response=None
        return response
        
def main():
    """This is te main method for the class DynamodbConfigSections"""
    parser = argparse.ArgumentParser(
        description="This argparser is to parameters for the methods in dynamodb separately"
    )
    subparsers = parser.add_subparsers()
    sub_create_table= subparsers.add_parser('create_table')
    sub_create_table.add_argument("parameters",ast.literal_eval)
    sub_put_item= subparsers.add_parser('put_item')
    sub_put_item.add_argument('table_name',type=str)
    sub_put_item.add_argument("parameters",ast.literal_eval)
    sub_get_item= subparsers.add_parser('get_item')
    sub_get_item.add_argument('table_name',type=str)
    sub_get_item.add_argument("parameters",ast.literal_eval)
    sub_update_item= subparsers.add_parser('update_item')
    sub_update_item.add_argument('table_name',type=str)
    sub_update_item.add_argument("parameters",ast.literal_eval)
    args = parser.parse_args()
    print(args)
    
if __name__ == "__main__":
    main()