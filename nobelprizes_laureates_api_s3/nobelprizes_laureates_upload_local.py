"""This module contains script for uploading data in json format in partiton path"""
import os
import configparser
import shutil

parent_dir = os.path.dirname(os.getcwd())
config = configparser.ConfigParser()
config.read(parent_dir + "/details.ini")


class NobelLaureateLocalUpload:
    """This class has methods for local operations"""

    def __init__(self, logger):
        self.logger = logger

    def upload_parition_s3_local(self, local_apipath, copy_source, file_name, partition_path):
        """This method uploads weatherdata in the partition path in the form of json"""
        try:
            new_dir = local_apipath + "/" + partition_path + "/"
            if not os.path.exists(new_dir):
                os.makedirs(new_dir)
            shutil.copy(copy_source, new_dir)
            print(copy_source)
            os.remove(copy_source)
            print("Successfully created json file in the given path\n")
            self.logger.info("Successfully created json file in the given path")

        except Exception as err:
            print("Cannot upload the json file in the given path:", err)
            self.logger.error("Cannot upload the json file in the given path")
            file_name = None
        return file_name
