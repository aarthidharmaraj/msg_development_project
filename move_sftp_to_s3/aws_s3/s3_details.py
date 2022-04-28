"""This module has S3 folder details and actions on that folder"""
import configparser
import boto3

# from fixtures import s3_mock

config = configparser.ConfigParser()
config.read("details.ini")


class S3Details:
    """This class has the methods for s3 service"""

    def __init__(self, logger_obj):
        """This is the init method of the class S3Service"""
        self.client = boto3.client(
            "s3",
            aws_access_key_id=config["s3"]["aws_access_key_id"],
            aws_secret_access_key=config["s3"]["aws_secret_access_key"],
        )
        # self.client=s3_mock()
        self.logger = logger_obj
        self.bucket_name = config["s3"]["bucket"]
        self.bucket_path = config["s3"]["bucket_path"]

    def upload_file(self, file, path):
        """This method gets file from sftp,uploads into s3 bucket in the given path"""
        self.client.put_object(Bucket=self.bucket_name, Body=file, Key=self.bucket_path + path)
        self.logger.info("The file has been uploaded to s3")
        return file
