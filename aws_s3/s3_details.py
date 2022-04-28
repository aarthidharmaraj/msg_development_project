"""This module has S3 folder details and actions on that folder"""
import configparser
# import os
import logging
# import shutil
import boto3
# from fixtures import s3_mock

config = configparser.ConfigParser()
config.read("details.ini")

logging.basicConfig(
    filename="sftptos3_logfile.log", format="%(asctime)s %(message)s", filemode="w"
)
logger = logging.getLogger()


class S3Details:
    """This class has the methods for s3 service"""

    def __init__(self):
        """This is the init method of the class S3Service"""
        self.client = boto3.client(
            "s3",
            aws_access_key_id=config["s3"]["aws_access_key_id"],
            aws_secret_access_key=config["s3"]["aws_secret_access_key"],
        )
        # self.client=s3_mock()
        self.bucket_name = config["s3"]["bucket"]
        self.s3_path = config["s3"]["bucket_path"]

    def upload_file(self,file, path):
        """This method gets file from sftp,uploads into s3 bucket in the given path"""
        # new_dir = self.s3_path + path
        # if not os.path.exists(new_dir):
        #     os.makedirs(new_dir)
        # shutil.copyfile(sftp_source, new_dir)
        self.client.put_object(
            Bucket=self.bucket_name, Body=file, Key=self.bucket_path + path
        )
        logger.info("The file has been uploaded to s3")
        return file
