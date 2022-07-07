"""This method gets the meta museum object data from api based on the given object id
which are available in api ,partition them based on object id and upload to s3"""
import os
import argparse
import sys
import pandas as pd
from logger_path.logger_object_path import LoggerPath
# from aws_s3.s3_details import S3Details
from metamuseum_temp_s3 import ApiDataPartitionUploadLocal
from metamuseum_api import MetaMuseumApi
from dynamodb_for_scripts_mocking import DynamodbConfigSections

datas = LoggerPath.logger_object("metamuseum_api")
api = MetaMuseumApi(datas["config"]["metamuseum_api"], datas["logger"])
def create_table_args():
        """This method returns the arguments for create table method"""
        parameters={'TableName':'config_section','KeySchema':[{'AttributeName': 'section_id','KeyType': 'HASH'},{'AttributeName': 'section_name','KeyType': 'RANGE'}], 'AttributeDefinitions':[{'AttributeName': 'section_id','AttributeType': 'N'},{'AttributeName': 'section_name','AttributeType': 'S' }],'ProvisionedThroughput':{'ReadCapacityUnits': 5,'WriteCapacityUnits': 5},}
        return parameters

def put_item_args():
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
    
class MetaMuseumObjectRecordS3:
    """This method fetches the record of an object from meta museum api based on
    the given object id,partition them and upload to s3"""

    def __init__(self):
        """This is the init method for the class PublicHolidaysLongWeekendS3"""
        
        self.dynamodb=DynamodbConfigSections()
        table_args=create_table_args()
        put_args=put_item_args()
        self.section = self.dynamodb.get_item_from_table_dynamodb('config_section',table_args,put_args,{'Key':{
            'section_id': 1,
            'section_name':'metamuseum_api'
        },
        })
        print(self.section)
        self.logger = datas["logger"]
        # self.s3_client = S3Details(self.logger, datas["config"])
        self.local = ApiDataPartitionUploadLocal(self.logger)
        self.api = MetaMuseumApi(self.section, self.logger)
        self.logger.info("Successfully created the instance of the class")

    def get_response_from_api(self, obj_id):
        """This method fetches the record of object from meta museum api based on the object id
        parameters :  obj_id - the id of object for which datas are needed"""
        try:
            for id in obj_id:
                print(id)
                self.logger.info("Getting response from api for %s id", id)
                response = self.api.get_endpoint_for_object_record(id)
                file_name = f"record_for_id{id}.json"
                self.get_dataframe_for_response(response, id, file_name)
        except Exception as err:
            self.logger.error(
                "Cannot able to get response from api for the given obj_id - %s", err
            )
            response = None
            print(err)
            sys.exit("System has terminated for fail in getting response from api")
        return response

    def get_dataframe_for_response(self, response, obj_id, file_name):
        """This method gets response from api,convert to dataframe and create json file for that
        parameters : response - response from api
                    obj_id - the id for which object records are fetched
                    file_name - file_name to be uploaded in s3"""
        try:
            if response is not None:
                self.logger.info("Got the response from api for %s", obj_id)
                df_data = pd.DataFrame.from_dict(response, orient="index")
                df_data = df_data.transpose()
                if not df_data.empty:
                    partition_path = self.put_partition_path(obj_id)
                    self.create_json_file_partition(df_data, file_name, partition_path, obj_id)
            else:
                self.logger.info("No responses from api for the %s", obj_id)

                df_data = None
                raise Exception
        except Exception as err:
            self.logger.error("Cannot create the dataframe for the given response %s", err)
            print("cannot get the response from api for the object id",obj_id)
            df_data = None
        return df_data

    def create_json_file_partition(self, df_data, file_name, partition_path, obj_id):
        """This method creates a temporary json file,create partition path and upload to s3
        parameters : df_data - dataframe created from the response
                    file_name - file_name to be uploaded in s3
                    partition_path - partition based on obj_id"""
        try:
            self.logger.info("Got the dataframe for the object id %s", obj_id)
            s3_path = self.section["s3_path"]
            local_path = LoggerPath.local_download_path("metamuseum_api_datas")
            df_data.to_json(
                local_path + "/" + file_name,
                orient="records",
                lines=True,
            )
            self.logger.info("Created the json file for %s", obj_id)
            copy_source = local_path + "/" + file_name
            file_name = self.local.upload_partition_s3_local(
                local_path,
                copy_source,
                file_name,
                s3_path + "/" + partition_path,
            )
            # file=self.upload_to_s3(s3_path,copy_source,s3_path  + "/" + partition_path, file_name)
        except Exception as err:
            self.logger.error("Cannot create a json file for file_name %s", file_name)
            print("Canot create a json file", err)
            file_name = None
        return file_name

    def put_partition_path(self, obj_id):
        """This method will make partion path based on obj_id
        and avoid overwrite of file and upload to local"""
        try:
            partition_path = f"pt_id={obj_id}"
            self.logger.info("Created the partition path for the given %s", obj_id)
        except Exception as err:
            self.logger.error("Cannot made a partition for %s because of %s", obj_id, err)
            partition_path = None
        return partition_path

    def upload_file_to_s3(self, bucket_path, source, partition_path, file_name):
        """This method used to upload the file to s3 in the partiton created
        parameters: file_name - name of the file to be uploaded
                    source - the source which has the file and its data
                    partition_path - path has partition based on endpoint and date"""
        try:
            key = partition_path + "/" + file_name
            self.logger.info(
                "The file %s being uploaded to s3 in the given path %s", file_name, key
            )
            # file = self.s3_client.upload_file(source, self.section["bucket"], bucket_path, key)
            print("the file has been uploaded to s3 in the given path")
            os.remove(source)
        except Exception as err:
            self.logger.error("Cannot upload the files to s3 in the given path %s", err)
            file = None
        return file

def main():
    """This is the main method for this module"""
    parser = argparse.ArgumentParser(
        description="This argparser is to get the object id to fetch objects data from meta museum api"
    )
    parser.add_argument("--objectid", help="Enter the object id", type=int, required=True,
        nargs="*")
    args = parser.parse_args()
    api_details = MetaMuseumObjectRecordS3()
    api_details.get_response_from_api(args.objectid)


if __name__ == "__main__":
    main()
