""" This module fetches the nobel prizes and laureates based on given year 
from api,partition them based on year and upload to s3 in that path"""

import logging
import configparser
import os
import argparse
from datetime import datetime
import sys
from time import time
import pandas as pd
from nobelprizes_laureates_upload_local import NobelLaureateLocalUpload
from pull_nobelprizes_laureates_from_api import NobelprizeLaureatesFromApi

# from aws_s3.s3_details import S3Details

parent_dir = os.path.dirname(os.getcwd())
config = configparser.ConfigParser()
config.read(parent_dir + "/details.ini")


class NobelPrizeLaureatesPartitionS3:
    """This class has methods to get Nobel prizes and laureates response
    from api,filter and create dataframe based on year and
    partition them on year and upload to s3"""

    def __init__(self, logger_obj, startyear, endyear):
        """This is the init method for the class NobelPrizeLaureatesPartitionS3"""
        self.logger = logger_obj
        self.startyear = startyear
        self.endyear = endyear
        self.local_path = os.path.join(
            parent_dir, config["local"]["data_path"], "nobel_laureates_data"
        )
        if not os.path.exists(self.local_path):
            os.makedirs(self.local_path)
        self.local = NobelLaureateLocalUpload(logger_obj)
        self.api = NobelprizeLaureatesFromApi(logger_obj)
        # self.s3_client = S3Details(logger_obj)

    def fetch_nobel_prize_details_from_api(self):
        """This method fetches the nobel prize details from the api for the given year"""
        pull_for = "nobelPrizes"
        response = self.api.pull_nobelprizes_laureates_from_api(
            pull_for, self.startyear, self.endyear
        )
        if response is not None:
            df_data = pd.DataFrame(response["nobelPrizes"])
            # print(df_data)
            self.create_dataframe_for_each_year(df_data, pull_for)
        else:
            self.logger.info("System gets terminated due to no nobelprize response from api")
            sys.exit("System terminated while getting nobelprize response from api")

    def fetch_laureate_details_from_api(self):
        """This method fetches the Laureate details from the api for the given year"""
        pull_for = "laureates"
        response = self.api.pull_nobelprizes_laureates_from_api(
            pull_for, self.startyear, self.endyear
        )
        if response is not None:
            # print(response)
            df_data = pd.DataFrame(response["laureates"])
            self.create_dataframe_for_each_year(df_data, pull_for)
        else:
            self.logger.info("System gets terminated due to no laureate response from api")
            sys.exit("System terminated while getting laureate response from api")

    def create_dataframe_for_each_year(self, df_data, pull_for):
        """This method create dataframe for each year"""
        while self.startyear <= self.endyear:
            new_data = self.filter_create_response_by_year(df_data, self.startyear, pull_for)
            self.create_json_file_partition(new_data, self.startyear, pull_for)
            self.startyear = self.startyear + 1

    def filter_create_response_by_year(self, df_data, year, pull_for):
        """This method filters and creates a newdataframe from the response
        based on year using pandas"""
        try:
            if pull_for == "nobelPrizes":
                new_df = df_data[(df_data["awardYear"].str.contains(str(year)))]
                if not new_df.empty:
                    new_dataframe = new_df
            else:
                for year in df_data["nobelPrizes"]:
                    print(year)
                    new_dataframe = None
        except Exception as err:
            print("Cannot filter the datas in dataframe due to this error:", err)
            self.logger.error("Cannot filter the datas in dataframe due to an error")
            new_dataframe = None
        return new_dataframe

    def create_json_file_partition(self, new_df, year, pull_for):
        """This method creates a temporary json file,create partition path and upload to s3"""
        try:
            epoch = int(time())
            file_name = f"{pull_for}_{epoch}.json"
            new_df.to_json(self.local_path + "/" + file_name, orient="records", lines=True)
            partition_path = self.put_partition_path(year, pull_for)
            copy_source = self.local_path + "/" + file_name
            file_name = self.local.upload_parition_s3_local(
                self.local_path, copy_source, file_name, partition_path
            )
            # self.upload_to_s3(copy_source,partition_path, file_name)
        except Exception as err:
            self.logger.error("Cannot create a json file")
            print("Canot create a json file",err)

    def put_partition_path(self, year, pull_for):
        """This method will make partion path based on year
        and avoid overwrite of file and upload to local"""
        try:
            if pull_for == "nobelPrizes":
                partition_path = f"nobel/source/prize/pt_year={year}"
                # print(partition_path)
            else:
                partition_path = f"nobel/source/laureate/pt_year={year}"
                # print(partition_path)

        except Exception as err:
            self.logger.error("Cannot made a parttiion")
            print("Cannot made a partition because of this error", err)
            partition_path = None
        return partition_path

    def upload_to_s3(self, source, partition_path, file_name):
        """This method used to upload the file to s3 in the partiton created"""
        key = partition_path + "/" + file_name
        self.logger.info("The file is being uploaded to s3 in the given path")
        print("the file has been uploaded to s3 in the given path")
        self.s3_client.upload_file(source, key)
        os.remove(source)


def main():
    """This is the main method for this module"""
    log_dir = os.path.join(parent_dir, config["local"]["log_path"], "nobel_laureate_api_log")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_file = os.path.join(log_dir, "nobel_laureate_logfile.log")

    logging.basicConfig(
        level=logging.INFO,
        filename=log_file,
        datefmt="%d-%b-%y %H:%M:%S",
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
        filemode="w",
    )
    logger_obj = logging.getLogger()
    parser = argparse.ArgumentParser(
        description="This argparser is to get the required date_of_joining need to filter"
    )
    parser.add_argument("--fromyear", help="Enter the start year YYYY", type=int, default=1901)
    parser.add_argument(
        "--toyear",
        help="Enter the end year YYYY",
        type=int,
        default=int(datetime.now().date().strftime("%Y")) - 1,
    )
    args = parser.parse_args()
    api_details = NobelPrizeLaureatesPartitionS3(logger_obj, args.fromyear, args.toyear)
    # api_details.fetch_nobel_prize_details_from_api()
    api_details.fetch_laureate_details_from_api()


if __name__ == "__main__":
    main()
