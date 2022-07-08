"""This module gets the arguments from the user using argparser for create table, put an
item in the table, get item from table and update an item in table in dynamodb for storing
the config values and use them in script"""
import argparse
import ast
from moto import mock_dynamodb
from dynamodb_tables import DynamoDB
from logger_path.logger_object_path import LoggerPath
datas=LoggerPath.logger_object("dynamodb")

class DynamodbConfigSections:
    """This class has methods to create table, update item,get item and put item for 
    storing config datas in dynamodb"""
    
    def __init__(self):
        """This is the init method for the class DynamodbConfigSections"""
        self.logger=datas["logger"]
        self.dynamodb=DynamoDB(self.logger)
        
    def create_table_args(self):
        """This method returns the arguments for create table method"""
        parameters={'TableName':'config_section','KeySchema':[{'AttributeName': 'section_id','KeyType': 'HASH'},{'AttributeName': 'section_name','KeyType': 'RANGE'}], 'AttributeDefinitions':[{'AttributeName': 'section_id','AttributeType': 'N'},{'AttributeName': 'section_name','AttributeType': 'S' }],'ProvisionedThroughput':{'ReadCapacityUnits': 5,'WriteCapacityUnits': 5},}
        return parameters
    
    def put_item_args(self):
        """This method returns the arguments for put item method in dynamodb"""
        parameters={'Item':{
        'section_id': 1,
        'section_name': 'metamuseum_api',
        'section_data':{
            's3_path' :'metamuseum_api/source/objects',
            'bucket': 'msg-practice-induction',
            'basic_url':'https://collectionapi.metmuseum.org/public/collection/v1/'
    }}
        }
        return parameters
    

    def get_item_args(self):
        """This method returns the arguments for get item method in dynamodb"""
        parameters={'Key':{
            'section_id': 1,
            'section_name':'metamuseum_api'
        },
        }
        return parameters
        
    def create_table(self,args):
        """This method creates the table in dynamodb with the given parameters of config datas"""
        try:
            table=self.dynamodb.create_table_dynamodb(args)
            self.logger.info("Created the table in dynamodb for the given parameters")
        except Exception as err:
            print(err)
            self.logger.error("Cannot create the table in dynamodb for the given parameters")
            table=None
        return table
    
    def put_item_in_table_dynamodb(self,table_name,args,kwargs):
        """This method puts an item in the table with the given parameters of config datas"""
        try:
            response=self.dynamodb.put_item_table(table_name,args,kwargs)
            print(response)
            self.logger.info("Inserted an item in table with the given parameters")
        except Exception as err:
            self.logger.error("Cannot insert the item in the table with the given parameters")
            print(err)
            response=None
        return response
    
    def get_item_from_table_dynamodb(self,table_name,table_args,put_args,get_args):
        """This method gets an item in the table with the given parameters of config datas"""
        try:
            response=self.dynamodb.get_item_from_table(table_name,table_args,put_args,get_args)
            self.logger.info("Got an item in table with the given parameters")
        except Exception as err:
            self.logger.error("Cannot get the item in the table with the given parameters")
            response=None
        return response
            
    def update_item_in_table_dynamodb(self,table_name,args):
        """This method Updates an item in the table with the given parameters of config datas"""
        try:
            response=self.dynamodb.update_item_from_table(table_name,args)
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
    sub_create_table.add_argument("param_create",type=ast.literal_eval)
    sub_put_item= subparsers.add_parser('put_item')
    sub_put_item.add_argument('table_name',type=str)
    sub_put_item.add_argument("param_put",type=ast.literal_eval)
    sub_get_item= subparsers.add_parser('get_item')
    sub_get_item.add_argument('table_name',type=str)
    sub_get_item.add_argument("param_get",type=ast.literal_eval)
    sub_update_item= subparsers.add_parser('update_item')
    sub_update_item.add_argument('table_name',type=str)
    sub_update_item.add_argument("param_update",type=ast.literal_eval)
    args = parser.parse_args()
    dynamo=DynamodbConfigSections()
    table_args=dynamo.create_table_args()
    if args.__dict__.get("param_create"):
        dynamo.create_table(args.param_create)
    if args.__dict__.get('param_put'):
        print(args.table_name,args.param_put)
        dynamo.put_item_in_table_dynamodb(args.table_name,table_args,args.param_put)
    elif args.__dict__.get("param_get"):
        put_args=dynamo.put_item_args()
        dynamo.get_item_from_table_dynamodb(args.table_name,table_args,put_args,args.param_get)
    # put_args=dynamo.put_item_args()
    # dynamo.put_item_in_table_dynamodb('config_section',table_args,put_args)
    # get_args=dynamo.get_item_args()
    # dynamo.get_item_from_table_dynamodb('config_section',table_args,put_args,get_args)
    
if __name__ == "__main__":
    main()