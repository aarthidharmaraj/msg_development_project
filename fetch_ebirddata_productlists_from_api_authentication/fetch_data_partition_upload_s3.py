"""This module get historical observations, top 100 contributors and
checklist feed from eBird Api based on region code and partition them based on
region,year, month,date and upload to s3"""

from datetime import datetime, timedelta
import os
import sys
import ast
import argparse
import itertools
import pandas as pd
import pycountry
from aws_s3.s3_details import S3Details
from logger_path.logger_object_path import LoggerPath
from fetch_data_product_upload_local import ApiDataPartitionUploadLocal
from get_response_from_api_with_authentication import PullDataFromEBirdApi

datas = LoggerPath.logger_object("ebird_api")
previous_date = datetime.now().date() - timedelta(days=1)


class HistoricDataProductsUploadS3:
    """This class has methods to get datas from api for the given date and
    partition them based on date and upload to s3"""

    def __init__(self, start_date, end_date, region, endpoint):
        """this is the init method for the class HistoricDataProductsUploadS3"""
        self.logger = datas["logger"]
        self.startdate = start_date
        self.enddate = end_date
        self.region_code = region
        self.endpoint = endpoint
        self.section = datas["config"]["eBird_api_datas"]
        self.logger.info("Successfully created the instance of the class")

    def check_for_endpoints_and_last_run_dates(self):
        """This method checks for the endpoints and their last run dates and gets the response as per the data"""
        try:
            if self.endpoint == "historical_data":
                ep_value = self.section["historical_data"]
                last_run = self.section["historic_last_run_date"]
                region_list = ast.literal_eval(self.section["historic_region_list"])
                endpoint_region = "historic_region_list"
                endpoint_date = "historic_last_run_date"
            elif self.endpoint == "top100_Contributors":
                ep_value = self.section["top100_Contributors"]
                last_run = self.section["top100_last_run_date"]
                region_list = ast.literal_eval(self.section["top100_region_list"])
                endpoint_region = "top100_region_list"
                endpoint_date = "top100_last_run_date"

            else:
                ep_value = self.section["checklist_feed"]
                last_run = self.section["checklist_last_run_date"]
                region_list = ast.literal_eval(self.section["checklist_region_list"])
                endpoint_date = "checklist_last_run_date"
                endpoint_region = "checklist_region_list"
            dates = self.get_details_for_givendates(last_run)
            self.check_for_regions_available(dates, ep_value, region_list, endpoint_region)
            self.last_date_of_execution(endpoint_date)
        except Exception as err:
            self.logger.error(
                "Cannot get the response for the endpoints for their corresponding dates %s", err
            )
            endpoint_date = None
        return endpoint_date

    def get_details_for_givendates(self, last_runned):
        """This method is to get the employee details for the given dates"""
        try:
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
                if last_run < self.startdate:
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
        except Exception as err:
            dates = None
            self.logger.info("Cannot get the details for the given dates %s", err)
            sys.exit("script was terminated when fetching for given dates")
        return dates

    def check_for_regions_available(self, dates, ep_value, available_regions, endpoint_region):
        """This method checks for the regions available, if it is new fetch from the start
        date,or else fetch for the given dates
        Parameters : endpoint - title of the endpoint
                    ep_value - The value of the endpoint to be fetched
                    available_regions - regions already present for the particular endpoints"""
        try:
            new_region = []
            if self.region_code:
                merged_region = list(itertools.chain(*self.region_code))
                for region in merged_region:
                    if region not in available_regions:  # check for the new region and fetch datas
                        new_region.append(region)
                        self.logger.info(
                            "Getting details for newly added region %s between %s and %s",
                            new_region,
                            dates["date1"],
                            dates["date2"],
                        )
                        available_regions.append(region)
                    else:
                        self.logger.info("The given region %s is already available", region)
                self.get_data_from_api_for_eachdate(
                    dates["date1"], dates["date2"], new_region, ep_value
                )
            else:
                self.get_data_from_api_for_eachdate(
                    dates["date1"], dates["date2"], available_regions, ep_value
                )
                self.logger.info(
                    "Getting details for available regions %s between %s and %s",
                    available_regions,
                    dates["date1"],
                    dates["date2"],
                )
            self.update_config_if_not_exixts(
                "eBird_api_datas", endpoint_region, str(available_regions)
            )
        except Exception as err:
            self.logger.error("Cannot fetch for the given region %s", err)
            print(err)
            new_region = None
        return new_region

    def get_data_from_api_for_eachdate(self, date1, date2, regions_list, ep_value):
        """This method gets data from api for each date between the start and end dates"""
        try:
            day_count = (date1 - date2).days + 1
            print(day_count)
            for day in range(day_count):  # Fetch for the given dates one by one
                single_date = date2 + timedelta(day)
                print(single_date)
                self.logger.info("Getting eBird api data for %s", single_date)
                # Fetch for the available regions
                for region in regions_list:
                    self.fetch_dataframe_from_api_for_endpoints(
                        region, single_date, self.endpoint, ep_value
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
            api = PullDataFromEBirdApi(self.logger, self.section)
            print(endpoint, "for", region_code)
            if endpoint == "historical_data":  # chek the endpoints and fetch data
                response = api.fetch_historical_data_from_api(date, region_code, ep_value)
            else:
                response = api.fetch_checklist_feed_top100_contributors_from_api(
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
            temp_s3 = ApiDataPartitionUploadLocal(self.logger)
            s3_path = self.section["s3_path"]
            local_path = LoggerPath.local_download_path("eBird_api_datas")
            partition_path = self.put_partition_path(s3_path, region, date, endpoint)
            file_name = f"{endpoint}_on_{date}_for_{region}.json"
            dataframe_data.to_json(
                local_path + "/" + file_name,
                orient="records",
                lines=True,
            )
            self.logger.info("Created the json file for %s on %s for %s", endpoint, date, region)
            copy_source = local_path + "/" + file_name
            self.logger.info("Uploading the json file to local s3 path")
            file = temp_s3.upload_partition_s3_local(
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
            s3_client = S3Details(self.logger, datas["config"])
            key = partition_path + "/" + file_name
            self.logger.info(
                "The file %s being uploaded to s3 in the given path %s", file_name, key
            )
            file = s3_client.upload_file(source, self.section["bucket"], bucket_path, key)
            print("the file has been uploaded to s3 in the given path")
            os.remove(source)
        except Exception as err:
            self.logger.error("Cannot upload the files to s3 in the given path %s", err)
            file = None
        return file

    def last_date_of_execution(self, endpoint_date):
        """If there is no end date,this method get last date of api run
        and upadates it to the config file"""
        if not self.enddate:
            last_run = datetime.now().date()
            self.update_config_if_not_exixts("eBird_api_datas", endpoint_date, str(last_run))
            self.logger.info("Last date has been updated in config file")
        else:
            self.logger.info("Last date cannot be updated because of presence of end date")
            last_run = None
        return last_run

    def update_config_if_not_exixts(self, section, endpoint_date, value):
        """This method updates the region in config if it does not exits"""
        try:
            datas["config"].set(section, endpoint_date, value)
            with open(datas["parent_dir"] + "/details.ini", "w", encoding="utf-8") as file:
                datas["config"].write(file)
            self.logger.info(
                "Updated or Added the given data %s in the available item in config file",
                value,
            )
            update = "Updated"
        except Exception as err:
            self.logger.error(
                "Cannot update the %s in section %s in config %s", value, section, err
            )
            update = "failed"
        return update


def check_valid_date(dates):
    """This method checks for the valid date entered and returns in datetime.date() format"""
    try:
        date = datetime.strptime(dates, "%Y-%m-%d").date()
        if date < datetime.now().date():  # checks with current date
            valid_date = date
            datas["logger"].info("The given date is a valid date %s", valid_date)
        else:
            datas["logger"].info("Cannot get datas from api for future dates")
            raise Exception
    except (Exception, ValueError):
        datas["logger"].info(
            "%s not valid date.It must be in format YYYY-MM-DD and not be current date or future dates",
            date,
        )
        valid_date = None
        msg = f"{date} not valid.It must be in format YYYY-MM-DD and not greater then current date"
        raise argparse.ArgumentTypeError(msg)
    return valid_date


def check_valid_region(codes):
    """This method checks the given region or country code is a valid country code or not"""
    try:
        available_codes = [co.alpha_2 for co in list(pycountry.countries)]
        if codes in available_codes:
            valid_code = codes
            datas["logger"].info("The given region is a valid region %s", valid_code)
        else:
            datas["logger"].info("Not a valid region code")
            raise Exception
    except Exception:
        datas["logger"].info("%s not valid region", codes)
        valid_code = None
        msg = f"{codes} not available region code.Cannot get data for that"
        raise argparse.ArgumentTypeError(msg)
    return valid_code


def main():
    """This is the main method for the class"""
    parser = argparse.ArgumentParser(
        description="This argparser is to gets date for fetching the datas from api"
    )
    parser.add_argument(
        "--startdate",
        help="The date should be in format YYYY-MM-DD",
        type=check_valid_date,
        default=previous_date,
    )
    parser.add_argument(
        "--enddate",
        help="The date should be in format YYYY-MM-DD",
        type=check_valid_date,
    )
    parser.add_argument(
        "endpoint",
        help="Choose the endpoint for response",
        choices=["historical_data", "top100_Contributors", "checklist_feed"],
        type=str,
    )
    parser.add_argument(
        "--regions",
        help="Enter the country code for which datas are to be fetched",
        nargs="*",
        type=check_valid_region,
        action="append",
    )
    args = parser.parse_args()
    api_data = HistoricDataProductsUploadS3(
        args.startdate, args.enddate, args.regions, args.endpoint
    )
    api_data.check_for_endpoints_and_last_run_dates()


if __name__ == "__main__":
    main()
