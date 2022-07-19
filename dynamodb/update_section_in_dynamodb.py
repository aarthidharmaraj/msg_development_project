"""This method is to get a section section from argparser, get datas from the config file
for that section, and update them in dynamodb table"""

import argparse
from logger_path.logger_object_path import LoggerPath
from dynamodb_table_item import DynamoDB

datas = LoggerPath.logger_object("config_dynamodb")

class ConfigDynamoDB:
    """This class has methods to get details from argparser and update them in dynamodb"""
    
    def __init__(self,table):
        """This is the init method for the class ConfigDynamoDB"""
        self.logger = datas["logger"]
        self.config=datas["config"]
        self.table_name=table
        self.dynamodb=DynamoDB(self.logger)
        
    def get_section_details_from_config(self,section_names):
        """This method gets the details present in the given section from config file"""
        try:
            for section in section_names:
                print("Getting data from section ",section)
                section_data=dict(self.config.items(section))
                self.logger.info("Got the data from the section %s ",section)
                self.update_to_dynamodb(section_data,section)
        except Exception as err:
            self.logger.error("CAnnot get the section's data from the config file %s",err)
            section_data=None
        return section_data
            
    def create_table_args(self):
        """This method returns the arguments for create table method"""
        parameters={'TableName':self.table_name,'KeySchema':[{'AttributeName': 'section_name','KeyType': 'HASH'}], 'AttributeDefinitions':[{'AttributeName': 'section_name','AttributeType': 'S' }],'ProvisionedThroughput':{'ReadCapacityUnits': 5,'WriteCapacityUnits': 5},}
        return parameters
    
    def put_item_params(self,section_data,section_name):
        """"This method cretaes the arguments for put item method"""
        put_parameters={'Item':{
            'section_name': section_name,
            'section_data':section_data
            }
                }
        return put_parameters
    
    def update_to_dynamodb(self,section_data,section_name):
        """This module get the sections data from config and update them to dynamodb"""
        try:
            table_params=self.create_table_args()
            put_params=self.put_item_params(section_data,section_name)
            self.dynamodb.put_item_table(self.table_name,table_params,put_params)
            get_params={'Key':{'section_name':section_name}}
            get_data=self.dynamodb.get_item_from_table(self.table_name,table_params,put_params,get_params)
            print(get_data)
            self.logger.info("Updated the section %s details in dynamodb", section_name)
        except Exception as err:
            self.logger.error("Cannot Updated the section %s details in dynamodb %s",section_name,err)
            get_data=None
        return get_data
    
        
def main():
    """This is the main method for the class ConfigDynamoDB"""
    parser = argparse.ArgumentParser(
        description="This argparser is to get the section name to get deatils from config"
    )
    parser.add_argument("--section_names", help="Enter the section_names", type=str,required=True,
        nargs="*")
    parser.add_argument("--table_name", help="Enter the table name", type=str,required=True)
    args = parser.parse_args()
    conf=ConfigDynamoDB(args.table_name)
    conf.get_section_details_from_config(args.section_names)

if __name__ == "__main__":
    main()