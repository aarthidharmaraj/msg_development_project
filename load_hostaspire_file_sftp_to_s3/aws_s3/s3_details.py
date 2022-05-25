"""This module has S3 folder details and actions on that folder"""
import boto3
from botocore.exceptions import ClientError

class S3Details:
    """This class has the methods for s3 service"""

    def __init__(self, logger, config):
        """This is the init method of the class S3Service"""
        self.client = boto3.client(
            "s3",
            aws_access_key_id=config["s3"]["aws_access_key_id"],
            aws_secret_access_key=config["s3"]["aws_secret_access_key"],
        )
        self.logger = logger

    def upload_file(self, file,bucket_name, bucket_path, key):
        """This method gets file from sftp,uploads into s3 bucket in the given path"""
        try:
            file = self.client.put_object(Bucket=bucket_name, Body=file, Key=bucket_path + key)
            self.logger.info("The file has been uploaded to s3")
        except Exception as err:
            print("Cannot upload in s3", err)
            self.logger.error("Cannot be uploaded in s3 in the given path")
            file = None
        return file

    def get_list_of_files_in_s3(self,bucket_name, bucket_path):
        """This method get the list of files from the given bucket and path in s3"""
        file_list = []
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            for objects in paginator.paginate(Bucket=bucket_name, Prefix=bucket_path):
                for obj in objects["Contents"]:
                    if obj["Key"].endswith(".zip"):
                        file_list.append(obj["Key"].split("/"[-1]))
            self.logger.info("Got the list of files from s3")
        except ClientError as err:
            self.logger.error("Cannot get the list of files from s3 in the given path %s", err)
            file_list = None
        return file_list