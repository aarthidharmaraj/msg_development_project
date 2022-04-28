"""This module has script for the performance on sftp folder"""
import configparser
import pysftp
from S3.s3_details import S3Details

# from fixtures import mock_sftp

config = configparser.ConfigParser()

config.read("details.ini")


class SftpConnection:
    """This class that contains methods for get file from sftp and renaming after uploded to s3"""

    def __init__(self, logger):
        """This is the init method of the class of SftpCon"""
        self.conn = pysftp.Connection(
            host=config["SFTP"]["host"],
            username=config["SFTP"]["username"],
            password=config["SFTP"]["password"],
        )
        # self.conn=mock_sftp
        self.logger = logger
        self.s3_client = S3Details(logger)
        self.sftp_path = config["SFTP"]["sftp_path"]

    def list_files(self):
        """This method gets the list of files names for the given sftp path by filtering"""
        sftp_file_list = [
            file for file in self.conn.listdir(self.sftp_path) if not file.startswith("prcssd.")
        ]

        return sftp_file_list

    def get_file(self, file, partition_path):
        """This method get file from sftp without downloading to local file"""
        try:
            with self.conn.open(self.sftp_path + file, "r") as file_name:
                file_name.prefetch()
                self.logger.info(
                    "The file has been fetched from sftp and passed upload method in s3"
                )
                self.s3_client.upload_file(file_name, partition_path)

        except IOError as ier:
            print("The file cannot be opened in sftp", ier)
            self.logger.error("The file cannot be opened in sftp")

    def rename_file(self, file):
        """This method is used for rename the sftp file in directory after processed"""
        new_name = self.sftp_path + "prcssd." + file
        self.conn.rename(self.sftp_path + file, new_name)
        print("The file", file, "in sftp has been processed and renamed successfully")
        self.logger.info("The file in sftp has been processed and renamed successfully")

        return new_name
