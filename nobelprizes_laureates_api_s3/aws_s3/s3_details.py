"""This module has S3 folder details and actions on that folder"""
import configparser
import boto3
from fixtures import s3_mock

config = configparser.ConfigParser()
config.read("details.ini")


class S3Details:
    """This class has the methods for s3 service"""

    def __init__(self, logger):
        """This is the init method of the class S3Service"""
        # self.client = boto3.client(
        #     "s3",
        #     aws_access_key_id=config["s3"]["aws_access_key_id"],
        #     aws_secret_access_key=config["s3"]["aws_secret_access_key"],
        # )
        self.client = s3_mock
        self.logger = logger
        # self.bucket_name = config["s3"]["bucket"]
        # self.bucket_path = config["s3"]["bucket_path"]

    def upload_file(self, file, path):
        """This method gets file from sftp,uploads into s3 bucket in the given path"""
        try:
            # self.client.put_object(Bucket=self.bucket_name, Body=file, Key=self.bucket_path + path)
            self.logger.info("The file has been uploaded to s3")
        except Exception as err:
            print("Cannot upload in s3", err)
            self.logger.error("Cannot be uploaded in s3 int he given path")
            file = None
        return file
