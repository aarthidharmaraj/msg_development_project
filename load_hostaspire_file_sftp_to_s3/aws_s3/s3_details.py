"""This module has S3 folder details and actions on that folder"""
import configparser
import boto3
from botocore.exceptions import ClientError
# from fixtures import s3_mock

class S3Details:
    """This class has the methods for s3 service"""

    def __init__(self, logger,config):
        """This is the init method of the class S3Service"""
        self.client = boto3.client(
            "s3",
            aws_access_key_id=config["s3"]["aws_access_key_id"],
            aws_secret_access_key=config["s3"]["aws_secret_access_key"],
        )
        # self.client = s3_mock
        self.logger = logger
        self.bucket_name = config["s3"]["bucket"]
        self.bucket_path = config["s3"]["bucket_path"]

    def upload_file(self, file, path):
        """This method gets file from sftp,uploads into s3 bucket in the given path"""
        try:
            file = self.client.put_object(
                Bucket=self.bucket_name, Body=file, Key=self.bucket_path + path
            )
            # self.client.upload_file(file,
            #     self.bucket_name,self.bucket_path+path
            # )
            self.logger.info("The file has been uploaded to s3")
        except Exception as err:
            print("Cannot upload in s3", err)
            self.logger.error("Cannot be uploaded in s3 in the given path")
            file = None
        return file

    def get_list_of_files_in_s3(self, prefix):
        """This method get the list of files from the given bucket and path in s3"""
        file_list = []
        try:
            objects = self.client.list_objects_v2(
                Bucket=self.bucket_name, prefix=self.bucket_path + prefix
            )
            for obj in objects["Contents"]:
                if obj["Key"].endswith(".zip"):
                    file_list.append(obj["Key"].split("/"[-1]))
        except ClientError as err:
            self.logger.error("Cannot get the list of files from s3 in the given path %s",err)
            file_list = None
        return file_list
