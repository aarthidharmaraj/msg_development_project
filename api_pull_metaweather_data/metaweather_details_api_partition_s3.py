"""This module pulls the weather results of each cities in US from 
given api "https://www.metaweather.com/api/" based on provided dates
and store them in S3 in the partitoned format """


import logging
import configparser
from datetime import datetime, timedelta
import argparse
import os
import re

# from sqlite3 import Timestamp
# from aws_s3.s3_details import S3Details
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
        self.local_apipath = os.path.join(
            parent_dir, config["local_metaweather_api"]["local_file_path"]
        )
        self.metaweather_cityapi = PullDataFromMetaweatherApi(logger_obj)
        self.local = MetaweatherPartitionLocal(logger_obj)
        # self.s3_client = S3Details(logger_obj)

    def check_for_presence_of_given_dates(self):
        """If there is a presence of enddate and startdate,it gets informationbetween them
        else gets end date alone orelse gets the information for last date and previous date"""
        if self.enddate:
            if self.enddate and self.startdate:
                self.logger.info(
                    "Getting metaweather info b/w the provided start date/previous date and enddate"
                )
                print("between start and end dates")
                self.get_metaweather_data_for_givendates(self.enddate, self.startdate)
            else:
                print("only for en date")
                self.logger.info("Getting metaweather information for the end date alone")
                self.get_metaweather_data_for_each_city(self.enddate)
        else:
            last_run_date = config["last_execution_of_api"]["last_run_date"]
            if last_run_date:
                last_run = datetime.strptime(last_run_date, "%Y-%m-%d").date()
                self.get_metaweather_data_for_givendates(last_run, self.startdate)
                self.logger.info(
                    "Getting metaweather information between lastdate and previous date"
                )
            else:
                self.logger.info("Getting metaweather information for the previous date alone")
                self.get_metaweather_data_for_each_city(self.startdate)

    def get_metaweather_data_for_givendates(self, date1, date2):
        """This method gets information between the two dates if given"""
        day_count = (date1 - date2).days + 1
        print(day_count)
        # for single_date in [d for d in (self.startdate + timedelta(n) for n in range(day_count))
        # if d <= self.enddate]:
        for day in range(day_count):
            single_date = self.startdate + timedelta(day)
            # print(single_date)
            self.get_metaweather_data_for_each_city(single_date)

    def get_metaweather_data_for_each_city(self, date):
        """This method gets data for the woeid of each city from api for the provided dates"""
        search_date = date.strftime("%Y/%m/%d")
        for key, value in self.woeid.items():
            response = self.metaweather_cityapi.get_weather_data_cities_using_woeid_from_api(
                value, search_date
            )
            # print(response)
            self.put_partition_path(response, key, date)
        return response

    def put_partition_path(self, response, key, date):
        """This method will make partion path based on city,year,month,date
        and hour and pass for uploading"""
        date_object = datetime.strptime(str(date), "%Y-%m-%d")
        date = date_object.strftime("%d")
        for data in response:
            values = re.split("_|-|T|:", data["created"])
            if values[2] == date:
                partition_path = "pt_city={city}/pt_year={year}/pt_month={month}/pt_day={day}/pt_hour={hour}/".format(
                    city=key,
                    year=values[0],
                    month=values[1],
                    day=values[2],
                    hour=values[3],
                )
                print(partition_path)
                # return partition_path
                self.local.upload_parition_s3_local(partition_path, data)
                # self.upload_to_s3(partition_path)
            else:
                self.logger.info("The provided dates and created dates are not matching")
                # print("We can partition only the created record match with provided dates",values[2],"is not a match")

    # def upload_to_s3(self, partition_path):
    #     """This method used to upload the file to s3 in the partiton created"""
    #     path = self.local_apipath + partition_path
    #     for file in os.listdir(path):
    #         response = self.s3_client.upload_file(file, path)
    #     return response

    def last_date_of_execution(self):
        """If there is no end date,this method get last date of api run
        and upadates it to the config file"""
        if not self.enddate:
            last_run = datetime.now().date()
            # last_run=Timestamp.now()
            config.set("last_execution_of_api", "last_run_date", str(last_run))
            with open(parent_dir + "/details.ini", "w", encoding="utf-8") as file:
                config.write(file)
            self.logger.info("Last date has been updated in config file")
        else:
            self.logger.info("Last date cannot be updated because of presence of end date")


def valid_date(date):
    """This method checks for the valid date entered and returns in datetime.date() format"""
    try:
        return datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError:
        msg = "not a valid date: {0!r}".format(date)
        raise argparse.ArgumentTypeError(msg)


def main():
    """This is the main method for this module"""
    log_dir = os.path.join(parent_dir, config["log_metaweather"]["log_path"])
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
    pull_metadata.check_for_presence_of_given_dates()
    pull_metadata.last_date_of_execution()


if __name__ == "__main__":
    main()
