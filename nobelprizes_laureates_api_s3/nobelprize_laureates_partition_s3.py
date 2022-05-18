""" This module fetches the nobel prizes and laureates based on given year 
from api,partition them based on year and upload to s3 in that partition_path"""

import logging
import configparser
import os
import argparse
from datetime import date
import sys
import pandas as pd
from nobelprizes_laureates_upload_local import NobelLaureateLocalUpload
from pull_nobelprizes_laureates_from_api import NobelprizeLaureatesFromApi
from aws_s3.s3_details import S3Details

parent_dir = os.path.dirname(os.getcwd())
config = configparser.ConfigParser()
config.read(parent_dir + "/details.ini")


class NobelPrizeLaureatesPartitionS3:
    """This class has methods to get Nobel prizes and laureates response
    from api,filter and create dataframe based on year and
    partition them on year and upload to s3"""

    def __init__(self, logger_obj, startyear, endyear, endpoint):
        """This is the init method for the class NobelPrizeLaureatesPartitionS3"""
        self.logger = logger_obj
        self.startyear = startyear
        self.endyear = endyear
        self.endpoint = endpoint
        self.local_path = os.path.join(
            parent_dir, config["local"]["data_path"], "nobel_laureates_data"
        )
        if not os.path.exists(self.local_path):
            os.makedirs(self.local_path)
        self.local = NobelLaureateLocalUpload(logger_obj)
        self.api = NobelprizeLaureatesFromApi(logger_obj)
        self.s3_client = S3Details(logger_obj)

    def fetch_nobelprize_laureate_from_api_each_year(self):
        """This method fetches the nobel prize and laureates details from the api
        for each year between the given years"""
        years_list = []
        try:
            if self.endyear:
                for get_year in range(self.startyear, self.endyear + 1):
                    years_list.append(get_year)
            else:
                years_list.append(self.startyear)
        except Exception as err:
            self.logger.error(f"Cannot get the resposnse from the given list of years{err}")
            print(err)
            years_list = None
        return years_list

    def get_response_from_api(self, years_list):
        """This method gets the nobel prizes and laureates response from api
        and convert to a dataframe using pandas"""
        try:
            for year in years_list:
                print(year)
                response = self.api.pull_nobelprizes_laureates_from_api(self.endpoint, year)
                if response is not None:
                    self.logger.info(f"Got the response from api for the year {year}")
                    if self.endpoint == "nobelPrizes":
                        df_data = pd.DataFrame(response["nobelPrizes"])
                    else:
                        df_data = pd.DataFrame(response["laureates"])
                    if not df_data.empty:
                        self.create_json_file_partition(df_data, year)
                else:
                    self.logger.info(f"No responses from api {response}")
                    df_data = None
        except Exception as err:
            self.logger.error(f"Cannot able to get response from api{err}")
            df_data = None
            sys.exit(
                "System has terminated for fail in getting nobelprize or laureate response from api"
            )
        return df_data

    def create_json_file_partition(
        self,
        df_data,
        award_year,
    ):
        """This method creates a temporary json file,create partition path and upload to s3"""
        try:
            file_name = f"pull_for_{award_year}.json"
            df_data.to_json(self.local_path + "/" + file_name, orient="records", lines=True)
            partition_path = self.put_partition_path(award_year)
            copy_source = self.local_path + "/" + file_name
            file_name = self.local.upload_parition_s3_local(
                self.local_path, copy_source, file_name, partition_path
            )
            # self.upload_to_s3(copy_source,partition_path, file_name)
        except Exception as err:
            self.logger.error(f"Cannot create a json file for{file_name}")
            print("Canot create a json file", err)
            file_name = None
        return file_name

    def put_partition_path(self, award_year):
        """This method will make partion path based on year
        and avoid overwrite of file and upload to local"""
        try:
            if self.endpoint == "nobelPrizes":
                partition_path = f"nobel/source/prize/pt_year={award_year}"
                print(partition_path)
            else:
                partition_path = f"nobel/source/laureate/pt_year={award_year}"
                print(partition_path)

        except Exception as err:
            self.logger.error(f"Cannot made a partition becausee of {err}")
            print("Cannot made a partition because of this error", err)
            partition_path = None
        return partition_path

    def upload_to_s3(self, source, partition_path, file_name):
        """This method used to upload the file to s3 in the partiton created"""
        key = partition_path + "/" + file_name
        self.logger.info(f"The file is {file_name} being uploaded to s3 in the given path")
        print("the file has been uploaded to s3 in the given path")
        self.s3_client.upload_file(source, key)
        os.remove(source)


# def valid_endpoint(endpoint):
#     """This method checks for the valid endpoint"""
#     try:
#         if endpoint=='nobelPrizes' or endpoint =='laureates':
#             return endpoint
#         else:
#             raise ValueError
#     except ValueError:
#         msg = f"Not a valid endpoint.It must be either nobelPrizes or laureates"
#         raise argparse.ArgumentTypeError(msg)


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
    parser.add_argument(
        "--fromyear",
        help="Enter the start year YYYY",
        type=int,
        default=int(date.today().strftime("%Y")) - 1,
    )
    parser.add_argument("--toyear", help="Enter the end year YYYY", type=int)
    parser.add_argument(
        "--endpoint",
        choices=["nobelPrizes", "laureates"],
        help="Choose the endpoint",
        required=True,
    )
    args = parser.parse_args()
    api_details = NobelPrizeLaureatesPartitionS3(
        logger_obj, args.fromyear, args.toyear, args.endpoint
    )
    list_year = api_details.fetch_nobelprize_laureate_from_api_each_year()
    api_details.get_response_from_api(list_year)


if __name__ == "__main__":
    main()
