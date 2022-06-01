"""This module get historical information, top 100 contributors and checklist feed from eBird Api based on
region code and partition them based on date and upload to s3"""

from datetime import datetime, timedelta
import os
import sys
import ast
import argparse
from aws_s3.s3_details import S3Details
from logger_path.logger_object_path import LoggerPath
from fetch_data_product_upload_local import ApiDataPartitionUploadLocal
from get_response_from_api_with_authentication import PullDataFromEBirdApi

datas = LoggerPath.logger_object("historicdata_products_from_api")


class HistoricDataProductsUploadS3:
    """This class has methods to get datas from api for the given date and
    partition them based on date and upload to s3"""

    def __init__(self, start_date, end_date):
        """this is the init method for the class HistoricDataProductsUploadS3"""
        self.logger = datas["logger"]
        self.startdate = start_date
        self.enddate = end_date
        self.region_code = ast.literal_eval(datas["config"]["eBird_api_datas"]["region_code"])
        self.s3_client = S3Details(self.logger, datas["config"])
        self.local = ApiDataPartitionUploadLocal(self.logger)
        self.local_path = LoggerPath.local_download_path("historicdata_products_from_api")
        self.api = PullDataFromEBirdApi(self.logger, datas["config"])
        self.s3path = datas["config"]["eBird_api_datas"]["s3_path"]
        self.bucket = datas["config"]["eBird_api_datas"]["bucket"]
        self.last_run = datas["config"]["eBird_api_datas"]["last_run_date"]
        self.endpoint = "historical_observations"

    def get_employee_details_for_givendates(self):
        """This method is to get the employee details for the given dates"""
        if self.enddate:
            if self.startdate > (datetime.now().date()) or self.enddate > (datetime.now().date()):
                self.logger.info("Cannot fetch the details for future dates")
                sys.exit("script was terminated when fetching for future dates")
            else:
                if self.startdate < self.enddate:
                    dates = {"date1": self.enddate, "date2": self.startdate}
                    self.logger.info(
                        "Getting details from api for given startdate %s to enddate %s",
                        self.startdate,
                        self.enddate,
                    )
                else:
                    self.logger.info("Cannot fetch details as startdate is greater than enddate")
                    sys.exit("script was terminated since startdate is greater than enddate")
        else:
            if self.last_run:
                last_run = datetime.strptime(self.last_run, "%Y-%m-%d").date()
                if last_run < self.startdate:
                    print(last_run)
                    dates = {"date1": self.startdate, "date2": last_run}
                    self.logger.info(
                        "Getting api datas between lastdate %s and current date %s",
                        last_run,
                        self.startdate,
                    )
                # dates = {"date1": self.last_run, "date2": self.startdate}
            elif self.startdate > (datetime.now().date()):
                dates = {"date1": self.startdate, "date2": self.startdate}
                self.logger.info("Getting datas from api for the current date alone")
                print("Getting data only for current date %s", self.startdate)
            else:
                dates = None
                self.logger.info("Cannot fetch the details for future dates")
                sys.exit("script was terminated when fetching for future dates")
        return dates

    def get_data_from_api_for_eachdate(self, date1, date2):
        """This method gets data from api for each date between the start and end dates"""
        try:
            day_count = (date1 - date2).days + 1
            print(day_count)
            for day in range(day_count):
                single_date = date2 + timedelta(day)
                print(single_date)
                self.logger.info("Getting eBird api data for %s", single_date)
                for key, value in self.region_code.items():
                    self.fetch_datafrmae_from_api_upload_s3(key, value, single_date)
        except Exception as err:
            self.logger.error("Cannot get the date %s", err)
            single_date = None
        return single_date

    def fetch_datafrmae_from_api_upload_s3(self, region, value, date):
        """This method fetches dataframe from api for all the endpoints and convert to
        json make partition and upload to s3
        parameters: region- the country on which datas to be fetched
                    value - the code for the country
                    date - the date for which datas are needed"""
        try:
            partition_path = self.put_partition_path(region, date)
            dataframe_data = self.api.fetch_historical_data_from_api(date, value)
            self.logger.info(
                "Got the dataframe for the endpoint %s on %s for %s", self.endpoint, date, region
            )
            file_name = f"{self.endpoint}_on_{date}.json"
            dataframe_data.to_json(
                self.local_path + "/" + file_name,
                orient="records",
                lines=True,
            )
            self.logger.info(
                "Created the json file for %s on %s for %s", self.endpoint, date, region
            )
            copy_source = self.local_path + "/" + file_name
            self.logger.info("Uploading the json file to local s3 path")
            file = self.local.upload_partition_s3_local(
                self.local_path,
                copy_source,
                file_name,
                partition_path,
            )
            # file=self.upload_to_s3(copy_source,partition_path, file_name)
        except Exception as err:
            self.logger.error(
                "Cannot create a json file for %s on %s for %s", self.endpoint, date, region
            )
            print("Canot create a json file and upload to s3 local", err)
            file = None
        return file

    def put_partition_path(self, region, date):
        """This method will make partion path based on year,month,date
        and avoid overwrite of file and upload to local
        parameters: region- the country on which datas to be fetched
                    date - the date for which datas are needed"""
        try:
            partition_path = date.strftime(
                f"{self.endpoint}/{region}/pt_year=%Y/pt_month=%m/pt_day=%d"
            )
            # print(partition_path)
            self.logger.info(
                "Made the partition based on endpoint,region and date %s", partition_path
            )
        except Exception as err:
            self.logger.error("Cannot made a partition %s", err)
            print("Cannot made a partition because of this error", err)
            partition_path = None
        return partition_path

    def upload_file_to_s3(self, source, partition_path, file_name):
        """This method used to upload the file to s3 in the partiton created
        parameters: file_name - name of the file to be uploaded
                    source - the source which has the file and its data
                    partition_path - path has partition based on region,endpoint and date"""
        try:
            key = partition_path + "/" + file_name
            self.logger.info(
                "The file %s being uploaded to s3 in the given path %s", file_name, key
            )
            print("the file has been uploaded to s3 in the given path")
            file = self.s3_client.upload_file(source, self.bucket, self.s3path, key)
            os.remove(source)
        except Exception as err:
            self.logger.error("Cannot upload the files to s3 in the given path %s", err)
            file = None
        return file

    def last_date_of_execution(self):
        """If there is no end date,this method get last date of api run
        and upadates it to the config file"""
        if not self.enddate:
            last_run = datetime.now().date()
            datas["config"].set("eBird_api_datas", "last_run_date", str(last_run))
            with open(datas["parent_dir"] + "/details.ini", "w", encoding="utf-8") as file:
                datas["config"].write(file)
            self.logger.info("Last date has been updated in config file")
        else:
            self.logger.info("Last date cannot be updated because of presence of end date")
            last_run = None
        return last_run


def valid_date(date):
    """This method checks for the valid date entered and returns in datetime.date() format"""
    try:
        date = datetime.strptime(date, "%Y-%m-%d").date()
        datas["logger"].info("The given date is a valid date %s",date)
    except ValueError:
        datas["logger"].info("%s is not a valid date.It must be in format YYYY-MM-DD",date)
        msg = f" {date} is not a valid year.It must be in format YYYY"
        date = None
        raise argparse.ArgumentTypeError(msg)
    return date


def main():
    """This is the main method for the class"""
    parser = argparse.ArgumentParser(
        description="This argparser is to gets date for fetching the datas from api"
    )
    parser.add_argument(
        "--startdate",
        help="The date should be in format YYYY-MM-DD",
        type=valid_date,
        default=datetime.now().date(),
    )
    parser.add_argument(
        "--enddate",
        help="The date should be in - format YYYY-MM-DD",
        type=valid_date,
    )
    args = parser.parse_args()

    api_data = HistoricDataProductsUploadS3(args.startdate, args.enddate)
    dates = api_data.get_employee_details_for_givendates()
    api_data.get_data_from_api_for_eachdate(dates["date1"], dates["date2"])
    api_data.last_date_of_execution()


if __name__ == "__main__":
    main()
