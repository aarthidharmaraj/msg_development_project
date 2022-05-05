"""This module pulls the weather results of each cities in US from 
    given api "https://www.metaweather.com/api/" based on provided dates
    and store them in S3 in the partitoned format """

import logging
import configparser
from datetime import datetime, timedelta
import argparse
import os
from time import time
import sys
import pandas as pd
from aws_s3.s3_details import S3Details
from pull_data_each_city_from_api import PullDataFromMetaweatherApi
from metaweather_info_partition_local import MetaweatherPartitionLocal

parent_dir = os.path.dirname(os.getcwd())
config = configparser.ConfigParser()
config.read(parent_dir + "/details.ini")


class PullMetaWeatherDataUploadS3:
    """This class helps to pull data and upload to s3 in the parttioned path"""

    def __init__(self, logger_obj, startdate, enddate):
        """This is the init method of the class of PullDataFromApi"""
        self.logger = logger_obj
        self.woeid = config["city_woeid"]
        self.startdate = startdate
        self.enddate = enddate
        self.local_apipath = os.path.join(parent_dir, config["metaweather_api"]["local_file_path"])
        self.metaweather_cityapi = PullDataFromMetaweatherApi(logger_obj)
        self.local = MetaweatherPartitionLocal(logger_obj)
        self.s3_client = S3Details(logger_obj)

    def check_for_presence_of_given_dates(self):
        """If there is a presence of enddate and startdate,it gets informationbetween them
        else gets end date alone orelse gets the information for last date and previous date"""
        last_run_date = config["metaweather_api"]["last_run_date"]
        last_run = datetime.strptime(last_run_date, "%Y-%m-%d").date()
        if self.enddate:
            if self.enddate and self.startdate and self.enddate > self.startdate:
                if self.enddate == datetime.now().date():
                    dates = {"date1": (self.enddate) - timedelta(days=1), "date2": self.startdate}
                    self.logger.info("Skip for current date")
                else:
                    self.logger.info(
                        "Getting metaweather info b/w the provided start date/previous date and enddate"
                    )
                    print("Between start and end dates")
                    dates = {"date1": self.enddate, "date2": self.startdate}
            else:
                print("only for end date")
                self.logger.info("Getting metaweather information for the end date alone")
                dates = {"date1": self.enddate, "date2": self.enddate}
        else:
            if last_run and last_run < self.startdate:
                dates = {"date1": self.startdate, "date2": last_run}
                self.logger.info(
                    "Getting metaweather information between lastdate and previous date"
                )
            else:
                self.logger.info("Getting metaweather information for the previous date alone")
                print("Getting data only for previous date")
                dates = {"date1": self.startdate, "date2": self.startdate}
        return dates

    def get_metaweather_data_for_givendates(self, date1, date2):
        """This method gets information between the two dates if given"""
        day_count = (date1 - date2).days + 1
        print(day_count)
        for day in range(day_count):
            single_date = date2 + timedelta(day)
            self.logger.info("Getting metaweather data for provided dates one by one")
            self.get_metaweather_data_for_each_city(single_date)
            # return single_date

    def get_metaweather_data_for_each_city(self, date):
        """This method gets data for the woeid of each city from api for the provided dates"""
        search_date = date.strftime("%Y/%m/%d")
        for key, value in self.woeid.items():
            response = self.metaweather_cityapi.get_weather_data_cities_using_woeid_from_api(
                value, search_date
            )
            if response is not None:
                self.filter_group_response_by_hour(response, key, date)
            else:
                print("Cannot fetch the data from api due to problem in api ")
                self.logger.error("Cannot fetch the data from api due to problem in api")
                sys.exit()

    def filter_group_response_by_hour(self, response, city_name, date):
        """This method filters and creates a dataframe from the response
        based on hour using pandas"""
        epoch = int(time())
        file_name = f"metaweather_{city_name}_{epoch}.json"
        df_data = pd.DataFrame(response)
        try:
            for i in range(24):
                time_hour = "{date}T{hour}".format(date=date, hour=str(i).zfill(2))
                new_df = df_data[(df_data.created.str.contains(time_hour))]
                if not new_df.empty:
                    filter_time = time_hour
                    partition_path = self.put_partition_path(city_name, filter_time)
                    self.local.upload_parition_s3_local(new_df, file_name, partition_path)
                    # self.upload_to_s3(new_df,partition_path, file_name)
                    # return True
        except Exception as err:
            print("Cannot filter the datas in dataframe due to this error:", err)
            self.logger.error("Cannot filter the datas in dataframe due to an error")
            # return False

    def put_partition_path(self, city_name, filter_time):
        """This method will make partion path based on city,year,month,date
        and hour and avoid overwrite of file and upload to local"""
        try:
            date = datetime.strptime(filter_time, "%Y-%m-%dT%H")
            partition_path = date.strftime(
                f"pt_city={city_name}/pt_year=%Y/pt_month=%m/pt_day=%d/pt_hour=%H/"
            )
            print(partition_path)
        except Exception as err:
            self.logger.error("Cannot made a parttiion")
            print("Cannot made a partition because of this error", err)
            partition_path = None
        return partition_path

    def upload_to_s3(self, new_df, partition_path, file_name):
        """This method used to upload the file to s3 in the partiton created"""
        file = new_df.to_json()
        # print(file)
        key = partition_path + "/" + file_name
        self.logger.info("The file is being uploaded to s3 in the given path")
        print("the file has been uploaded to s3 in the given path")
        self.s3_client.upload_file(file, key)

    def last_date_of_execution(self):
        # def last_date_of_execution(self,end_date):
        """If there is no end date,this method get last date of api run
        and upadates it to the config file"""
        if not self.enddate:
            # if not end_date:
            last_run = datetime.now().date()
            config.set("metaweather_api", "last_run_date", str(last_run))
            with open(parent_dir + "/details.ini", "w", encoding="utf-8") as file:
                config.write(file)
            self.logger.info("Last date has been updated in config file")

        else:
            self.logger.info("Last date cannot be updated because of presence of end date")
            last_run = None
        return last_run


def valid_date(date):
    """This method checks for the valid date entered and returns in datetime.date() format"""
    try:
        return datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError:
        msg = "not a valid date: {0!r}".format(date)
        raise argparse.ArgumentTypeError(msg)


def main():
    """This is the main method for this module"""
    log_dir = os.path.join(parent_dir, config["metaweather_api"]["log_path"])
    log_file = os.path.join(log_dir, "metaweather_logfile.log")

    logging.basicConfig(
        level=logging.INFO,
        filename=log_file,
        datefmt="%d-%b-%y %H:%M:%S",
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
        filemode="w",
    )
    logger_obj = logging.getLogger()
    parser = argparse.ArgumentParser(
        description="Provide Start and end date for getting metaweather datas srom api"
    )
    parser.add_argument(
        "--startdate",
        help="The Start Date - format YYYY-MM-DD",
        type=valid_date,
        default=datetime.now().date() - timedelta(days=1),
    )
    parser.add_argument("--enddate", help="The End Date - format YYYY-MM-DD", type=valid_date)
    args = parser.parse_args()
    pull_metadata = PullMetaWeatherDataUploadS3(logger_obj, args.startdate, args.enddate)
    dates = pull_metadata.check_for_presence_of_given_dates()
    pull_metadata.get_metaweather_data_for_givendates(dates["date1"], dates["date2"])
    pull_metadata.last_date_of_execution()


if __name__ == "__main__":
    main()
