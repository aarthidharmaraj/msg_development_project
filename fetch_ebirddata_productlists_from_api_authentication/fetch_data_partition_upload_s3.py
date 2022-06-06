"""This module get historical observations, top 100 contributors and
checklist feed from eBird Api based on region code and partition them based 
on date and upload to s3"""

from datetime import datetime,date,timedelta
import os
import sys
import ast
import argparse
import pandas as pd
from aws_s3.s3_details import S3Details
from logger_path.logger_object_path import LoggerPath
from fetch_data_product_upload_local import ApiDataPartitionUploadLocal
from get_response_from_api_with_authentication import PullDataFromEBirdApi

datas = LoggerPath.logger_object("historicdata_products_from_api")
current_date = datetime.now().date()
initial_run_date=date(date.today().year, 1, 1)

class HistoricDataProductsUploadS3:
    """This class has methods to get datas from api for the given date and
    partition them based on date and upload to s3"""

    def __init__(self, start_date, end_date, region):
        """this is the init method for the class HistoricDataProductsUploadS3"""
        self.logger = datas["logger"]
        self.startdate = start_date
        self.enddate = end_date
        self.region_code = region
        self.section = datas["config"]["eBird_api_datas"]
        self.s3_client = S3Details(self.logger, datas["config"])
        self.local = ApiDataPartitionUploadLocal(self.logger)
        self.api = PullDataFromEBirdApi(self.logger, self.section)
        self.logger.info("Successfully created the instance of the class")

    def get_details_for_givendates(self):
        """This method is to get the employee details for the given dates"""
        try:
            last_runned = self.section["last_run_date"]
            # check the given dates ranges
            if self.enddate:
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
            elif last_runned:
                last_run = datetime.strptime(last_runned, "%Y-%m-%d").date()
                if last_run <= self.startdate:
                    dates = {"date1": self.startdate, "date2": last_run}
                    self.logger.info(
                        "Getting api datas between lastdate %s and current date %s",
                        last_run,
                        self.startdate,
                    )
                else:
                    self.logger.info("Cannot fetch details as lastdate is greater than startdate")
                    dates = {"date1": self.startdate, "date2": self.startdate}
                    self.logger.info("Fetching details for startdate alone")
            else:
                dates = {"date1": current_date, "date2": initial_run_date}
                self.logger.info(
                    "Fetching datas from api between 2022-01-01 and current date %s", current_date
                )
        except Exception as err:
            dates = None
            self.logger.info("Cannot get the details for the given dates %s", err)
            print("script was terminated when fetching for given dates")
        return dates

    def check_for_regions_available(self, dates):
        """This method checks for the regions available, if it is new fetch from the start
        date,or else fetch for the given dates
        Parameters : endpoint - title of the endpoint
                    ep_value - The value of the endpoint to be fetched"""
        try:
            new_region = []
            available_regions = self.section["region"]
            print(available_regions)
            # check for the new region and fetch datas
            if self.region_code and self.region_code not in available_regions:
                new_region.append(self.region_code)
                self.logger.info(
                    "Getting details for newly added region %s between current date and 2020-01-01",
                    new_region,
                )
                self.get_data_from_api_for_eachdate(
                    current_date,
                    initial_run_date,
                    new_region,
                )
            code_list = available_regions.split(",")
            self.get_data_from_api_for_eachdate(dates["date1"], dates["date2"], code_list)
        except Exception as err:
            self.logger.error("Cannot fetch for the given region %s", err)
            print(err)
            code_list = None
        return code_list

    def get_data_from_api_for_eachdate(self, date1, date2, regions_list):
        """This method gets data from api for each date between the start and end dates"""
        try:
            day_count = (date1 - date2).days + 1
            print(day_count)
            for day in range(day_count):  # Fetch for the given dates one by one
                single_date = date2 + timedelta(day)
                print(single_date)
                self.logger.info("Getting eBird api data for %s", single_date)
                endpoint = ast.literal_eval(self.section["endpoint"])
                for epoint, ep_value in endpoint.items():  # Fetch for the available endpoints
                    for region in regions_list:
                        self.fetch_dataframe_from_api_for_endpoints(
                            region, single_date, epoint, ep_value
                        )
        except Exception as err:
            self.logger.error("Cannot get the date %s", err)
            single_date = None
        return single_date

    def fetch_dataframe_from_api_for_endpoints(self, region_code, date, endpoint, ep_value):
        """This method fetches dataframe from api for all the endpoints and convert to
        json make partition and upload to s3
        parameters: region and region_code- the country and its code on which datas to be fetched
                    endpoint and ep_value - the endpoint and its value-path
                    date - the date for which datas are needed"""
        try:
            print(endpoint, "for", region_code)
            if endpoint == "historical_data":  # chek the endpoints and fetch data
                response = self.api.fetch_historical_data_from_api(date, region_code, ep_value)
            else:
                response = self.api.fetch_checklist_feed_top100_contributors_from_api(
                    date, region_code, ep_value
                )
            if response is not None:  # create dataframe if response is not None
                df_data = pd.DataFrame(response)
                self.create_json_upload_s3(df_data, date, region_code, endpoint)
            else:
                print("response is None")
                df_data = None
                self.logger.info("Cannot create a dataframe for an empty response %s", response)
        except Exception as err:
            self.logger.error("Cannot get the response from api %s", err)
            df_data = None
        return df_data

    def create_json_upload_s3(self, dataframe_data, date, region, endpoint):
        """This method creates the json file for the dataframe and upload to s3"""
        try:
            self.logger.info(
                "Got the dataframe for the endpoint %s on %s for %s", endpoint, date, region
            )
            s3_path = self.section["s3_path"]
            local_path = LoggerPath.local_download_path("eBird_api_datas")
            partition_path = self.put_partition_path(s3_path, region, date, endpoint)
            file_name = f"{endpoint}_on_{date}.json"
            dataframe_data.to_json(
                local_path + "/" + file_name,
                orient="records",
                lines=True,
            )
            self.logger.info("Created the json file for %s on %s for %s", endpoint, date, region)
            copy_source = local_path + "/" + file_name
            self.logger.info("Uploading the json file to local s3 path")
            file = self.local.upload_partition_s3_local(
                local_path,
                copy_source,
                file_name,
                partition_path,
            )
            # file=self.upload_to_s3(s3_path,copy_source,partition_path, file_name)
        except Exception as err:
            self.logger.error(
                "Cannot create a json file for %s on %s for %s", endpoint, date, region
            )
            print("Canot create a json file and upload to s3 local", err)
            file = None
        return file

    def put_partition_path(self, s3_path, region, date, endpoint):
        """This method will make partion path based on year,month,date
        and avoid overwrite of file and upload to local
        parameters: region- the country on which datas to be fetched
                    date - the date for which datas are needed"""
        try:
            partition_path = date.strftime(
                f"{s3_path}/{endpoint}/pt_region={region}/pt_year=%Y/pt_month=%m/pt_day=%d"
            )
            self.logger.info(
                "Made the partition based on endpoint,region and date %s", partition_path
            )
        except Exception as err:
            self.logger.error("Cannot made a partition %s", err)
            print("Cannot made a partition because of this error", err)
            partition_path = None
        return partition_path

    def upload_file_to_s3(self, bucket_path, source, partition_path, file_name):
        """This method used to upload the file to s3 in the partiton created
        parameters: file_name - name of the file to be uploaded
                    source - the source which has the file and its data
                    partition_path - path has partition based on region,endpoint and date"""
        try:
            key = partition_path + "/" + file_name
            self.logger.info(
                "The file %s being uploaded to s3 in the given path %s", file_name, key
            )
            file = self.s3_client.upload_file(source, self.section["bucket"], bucket_path, key)
            print("the file has been uploaded to s3 in the given path")
            os.remove(source)
        except Exception as err:
            self.logger.error("Cannot upload the files to s3 in the given path %s", err)
            file = None
        return file

    def last_date_of_execution(self):
        """If there is no end date,this method get last date of api run
        and upadates it to the config file"""
        if not self.enddate:
            last_run = current_date
            datas["config"].set("eBird_api_datas", "last_run_date", str(last_run))
            with open(datas["parent_dir"] + "/details.ini", "w", encoding="utf-8") as file:
                datas["config"].write(file)
            self.logger.info("Last date has been updated in config file")
        else:
            self.logger.info("Last date cannot be updated because of presence of end date")
            last_run = None
        return last_run

    def update_config_if_region_not_exixts(self):
        """This method updates the region in config if it does not exits"""
        try:
            available_code = self.section["region"]
            if (
                self.region_code in available_code
            ):  # check the given region is available orelse update it
                regions = None
                self.logger.info("The given region is available in the list")
            else:
                regions = self.section.get("region")
                datas["config"].set("eBird_api_datas", "region", regions + "," + self.region_code)
                with open(datas["parent_dir"] + "/details.ini", "w", encoding="utf-8") as file:
                    datas["config"].write(file)
                self.logger.info(
                    "Added the new region %s in the available regions in config file",
                    self.region_code,
                )
        except Exception as err:
            self.logger.error("Cannot update the new region in config %s", err)
            regions = None
        return regions


def check_valid_date(date):
    """This method checks for the valid date entered and returns in datetime.date() format"""
    try:
        date = datetime.strptime(date, "%Y-%m-%d").date()
        if date <= current_date:  # checks with current date
            valid_date = date
            datas["logger"].info("The given date is a valid date %s", valid_date)
        else:
            datas["logger"].info("Cannot get datas from api for future dates")
            raise Exception
    except (Exception, ValueError):
        datas["logger"].info(
            "%s not valid date.It must be in format YYYY-MM-DD and not greater then current date",
            date,
        )
        valid_date = None
        msg = f"{date} not valid.It must be in format YYYY-MM-DD and not greater then current date"
        raise argparse.ArgumentTypeError(msg)
    return valid_date


def main():
    """This is the main method for the class"""
    parser = argparse.ArgumentParser(
        description="This argparser is to gets date for fetching the datas from api"
    )
    parser.add_argument(
        "--startdate",
        help="The date should be in format YYYY-MM-DD",
        type=check_valid_date,
        default=current_date,
    )
    parser.add_argument(
        "--enddate",
        help="The date should be in format YYYY-MM-DD",
        type=check_valid_date,
    )
    parser.add_argument(
        "--region", help="Enter the country code for which datas are to be fetched", type=str
    )
    args = parser.parse_args()
    api_data = HistoricDataProductsUploadS3(args.startdate, args.enddate, args.region)
    dates = api_data.get_details_for_givendates()
    api_data.check_for_regions_available(dates)
    api_data.update_config_if_region_not_exixts()
    api_data.last_date_of_execution()


if __name__ == "__main__":
    main()
