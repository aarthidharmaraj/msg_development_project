"""This module contains the Operations which are performrnd in local"""
import os
import configparser
import json

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

    def upload_parition_s3_local(self, partition_path, weather_data):
        """This method uploads weatherdata in the partition path in the form of json"""
        new_dir = self.local_apipath + partition_path
        # file_name=values[3]+" : "+values[2]+"-"+ values[1]+"-"+values[0]
        if not os.path.exists(new_dir):
            os.makedirs(new_dir)
        with open(new_dir + "weather_data.json", "w", encoding="utf-8") as file:
            json.dump(weather_data, file)
            print("Successfully created json file in the given path\n")
            self.logger.info("Successfully created json file in the given path")
