"""This module has the script for moving files from sftp to s3
in the parition path based on year, month,date"""
import configparser
import re
import os
import shutil
from datetime import datetime
import logging
from aws_s3.s3_details import S3Details
from sftp.sftp_connection import SftpConnection

config = configparser.ConfigParser()
config.read("details.ini")


# from logging.handlers import TimedRotatingFileHandler
# h = TimedRotatingFileHandler('/home/user/Desktop/myLogFile.log')


class MoveSftpToS3:
    """This is the class for moving file sftp to s3"""

    def __init__(self, logger):
        """This is the init method for the class of MoveFileSftpToS3"""
        self.logger = logger
        # self.sftp_conn = SftpConnection(logger)
        # self.s3_client = S3Details(logger)
        self.local = MoveSftpToS3Local(logger)
        self.sftp_path = config["SFTP"]["sftp_path"]

    def move_file_to_s3(self):
        """This method moves the file from sftp to s3"""
        # sftp_file_list = self.sftp_conn.list_files()
        sftp_file_list = self.local.list_files_sftp_local()
        for file_name in sftp_file_list:
            copy_source = self.sftp_path + file_name
            self.put_path_partition(file_name, copy_source)
        return "success"

    def put_path_partition(self, file_name, sftp_source):
        """This method uses partioning of path and upload the file to S3"""
        try:
            if re.search("[0-9]{1,2}.[0-9]{1,2}.[0-9]{1,4}.(csv)", file_name):

                date_object = datetime.strptime(file_name, "%d.%m.%Y.csv")
                partition_path = (
                    "pt_year="
                    + date_object.strftime("%Y")
                    + "/pt_month="
                    + date_object.strftime("%m")
                    + "/pt_day="
                    + date_object.strftime("%d")
                )
                # self.sftp_conn.get_file(file_name, partition_path,sftp_source)
                # self.s3_client.upload_file(file_name, partition_path, sftp_source)
                self.local.upload_file_s3_local(file_name, partition_path, sftp_source)
                # rename = self.sftp_conn.rename_file(file_name)
                rename = self.local.rename_file_sftp_local(file_name)
                print("The file has been uploaded to s3 and rename in sftp path", rename)
                self.logger.info("The file has been uploaded to s3 and rename in sftp path")
                return "success"
            # else:
            #     print("The", file_name, "is not in the prescribed format")
            #     self.logger.error("The file is not in the prescribed format")

        except Exception as err:
            print("Cannot be uploaded in S3 in the parttioned path", err)
            self.logger.error("The file cannot be uploaded in the given path in s3")
        return "failure"


class MoveSftpToS3Local:
    """This class has the methods that do in local folder"""

    def __init__(self, logger):
        self.logger = logger
        self.s3_path = config["local_s3"]["s3_path"]
        self.sftp_path = config["local_sftp"]["sftp_path"]

    def upload_file_s3_local(self, file, path, sftp_source):
        """This method gets file from sftp,uploads into local s3 path"""
        new_dir = self.s3_path + path
        if not os.path.exists(new_dir):
            os.makedirs(new_dir)
        # shutil.copyfile(sftp_source, new_dir)
        print("the file", file, "has been uploaded in local s3_path with the paritition")
        self.logger.info("The file has been uploaded to s3")
        return file

    def list_files_sftp_local(self):
        """This method gets the list of files names for the given local sftp path by filtering"""
        sftp_file_list = [
            files for files in os.listdir(self.sftp_path) if not files.startswith("prcssd.")
        ]
        return sftp_file_list

    def rename_file_sftp_local(self, file):
        """This method is used for rename the sftp file or directory after processed"""
        new_name = self.sftp_path + "prcssd." + file
        os.rename(self.sftp_path + file, new_name)
        print("The file", file, "in sftp has been processed and renamed successfully")
        self.logger.info("The file in sftp has been processed and renamed successfully")
        # return 'success'
        return new_name


def main():
    """This is the main method for the module name move_file_sftp_to_s3"""
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%a, %d %b %Y %H:%M:%S",
        filename=config["log_sftptos3"]["log_path"],
        filemode="w",
    )
    logger_obj = logging.getLogger()
    move_sftp_to_s3 = MoveSftpToS3(logger_obj)
    move_sftp_to_s3.move_file_to_s3()


if __name__ == "__main__":
    main()
