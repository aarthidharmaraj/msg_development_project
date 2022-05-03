"""This module contains script for uploading data in json format in partiton path"""
import os
import configparser

parent_dir = os.path.dirname(os.getcwd())
config = configparser.ConfigParser()
config.read(parent_dir + "/details.ini")


class MetaweatherPartitionLocal:
    """This class has methods for local operations"""

    def __init__(self, logger):
        self.logger = logger
        self.local_apipath = os.path.join(
            parent_dir, config["local_metaweather_api"]["local_file_path"]
        )

    def upload_parition_s3_local(self, new_df, file_name, partition_path):
        """This method uploads weatherdata in the partition path in the form of json"""
        new_dir = self.local_apipath + "/" + partition_path + "/"
        if not os.path.exists(new_dir):
            os.makedirs(new_dir)
        new_df.to_json(new_dir + file_name, orient="records", lines=True)
        print("Successfully created json file in the given path\n")
        self.logger.info("Successfully created json file in the given path")
        return "json file created"
