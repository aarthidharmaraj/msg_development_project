"""This method gets the Public holidays, Long weekend details from api based on the given year
and Country which are available in api partition them based on year,region and upload to s3"""
import os
import argparse
from datetime import date
import sys
import pandas as pd
from logger_path.logger_object_path import LoggerPath
from aws_s3.s3_details import S3Details
from fetch_data_partition_upload_local import ApiDataPartitionUploadLocal
from get_response_from_holidayapi import HolidayApi

datas = LoggerPath.logger_object("holiday_api")
api = HolidayApi(datas["config"]["holiday_api"], datas["logger"])


class PublicHolidaysLongWeekendS3:
    """This method fetches the public holidays and Long weekend data from holiday api based on
    the given region and year,partition them and upload to s3"""

    def __init__(self, startyear, endyear, region):
        """This is the init method for the class PublicHolidaysLongWeekendS3"""
        self.startyear = startyear
        self.endyear = endyear
        self.region = region
        self.section = datas["config"]["holiday_api"]
        self.logger = datas["logger"]
        self.s3_client = S3Details(self.logger, datas["config"])
        self.local = ApiDataPartitionUploadLocal(self.logger)
        self.api = HolidayApi(self.section, self.logger)
        self.logger.info("Successfully created the instance of the class")

    def get_year_list_from_given_years(self):
        """This method gets the list of years from the given start year and end year"""
        years_list = []
        try:
            if self.endyear:
                for get_year in range(
                    self.startyear,
                    self.endyear + 1,
                ):
                    years_list.append(get_year)
            else:
                years_list.append(self.startyear)
            self.logger.info("Created a list for the given years%s", years_list)
        except Exception as err:
            self.logger.error("Cannot get the list of years %s", err)
            print(err)
            years_list = None
        return years_list

    def get_response_from_api(self, years_list, endpoint):
        """This method gets public Holidays/Long Weekend or Next public holidays response from api
        parameters :  years_list - The list of years for which datas are needed"""
        try:
            for year in years_list:
                self.logger.info("Getting response from api for %s for the %s", endpoint, year)
                response = self.api.get_endpoint_for_public_holidays_long_weekend(
                    year, self.region, endpoint
                )
                file_name = f"{endpoint}_on_{year}.json"
                partition_path = self.put_partition_path(year, endpoint)
                self.get_dataframe_for_response(response, year, file_name, partition_path, endpoint)
        except Exception as err:
            self.logger.error(
                "Cannot able to get response from api for the endpoint%s - %s", endpoint, err
            )
            response = None
            print(err)
            sys.exit("System has terminated for fail in getting response from api")
        return response

    def get_dataframe_for_response(self, response, year, file_name, partition_path, endpoint):
        """This method gets response from api,convert to dataframe and create json file for that
        parameters : response - response from api
                    year - year for which data is fetched
                    file_name - file_name to be uploaded in s3
                    partition_path - partition based on year and region"""
        try:
            if response is not None:
                self.logger.info("Got the response from api for %s", year)
                df_data = pd.DataFrame(response)
                if not df_data.empty:
                    self.create_json_file_partition(
                        df_data, year, file_name, partition_path, endpoint
                    )
            else:
                self.logger.info("No responses from api for the %s on %s", endpoint, year)
                df_data = None
        except Exception as err:
            self.logger.error("Cannot create the dataframe for the given response %s", err)
            df_data = None
        return df_data

    def create_json_file_partition(self, df_data, year, file_name, partition_path, endpoint):
        """This method creates a temporary json file,create partition path and upload to s3
        parameters : df_data - dataframe created from the response
                    year - year for which data is fetched
                    file_name - file_name to be uploaded in s3
                    partition_path - partition based on year and region"""
        try:
            self.logger.info("Got the dataframe for the endpoint %s", endpoint)
            s3_path = self.section["s3_path"]
            local_path = LoggerPath.local_download_path("holiday_api_datas")
            df_data.to_json(
                local_path + "/" + file_name,
                orient="records",
                lines=True,
            )
            self.logger.info("Created the json file for %s on %s", endpoint, year)
            copy_source = local_path + "/" + file_name
            file_name = self.local.upload_partition_s3_local(
                local_path,
                copy_source,
                file_name,
                s3_path + "/" + endpoint + "/" + partition_path,
            )
            # file=self.upload_to_s3(s3_path,copy_source,s3_path+"/"+endpoint+"/"+partition_path, file_name)
        except Exception as err:
            self.logger.error("Cannot create a json file for file_name %s", file_name)
            print("Canot create a json file", err)
            file_name = None
        return file_name

    def put_partition_path(self, year, endpoint):
        """This method will make partion path based on year
        and avoid overwrite of file and upload to local"""
        try:
            partition_path = f"pt_region={self.region}/pt_year={year}"
            self.logger.info("Created the partition path for the given %s and %s", endpoint, year)
        except Exception as err:
            self.logger.error("Cannot made a partition because of %s", err)
            partition_path = None
        return partition_path

    def upload_file_to_s3(self, bucket_path, source, partition_path, file_name):
        """This method used to upload the file to s3 in the partiton created
        parameters: file_name - name of the file to be uploaded
                    source - the source which has the file and its data
                    partition_path - path has partition based on endpoint and date"""
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


def valid_year(year):
    """This method checks for the valid endpoint"""
    try:
        if year.isdigit() and len(year) == 4:
            year_valid = int(year)
            datas["logger"].info("The given year  %s is a valid", year)
        else:
            raise ValueError
    except ValueError:
        datas["logger"].info("%s is not a valid year.It must be in format YYYY", year)
        msg = f" {year} is not a valid year.It must be in format YYYY"
        year_valid =None
        # raise argparse.ArgumentTypeError(msg)
    return year_valid


def valid_region(code):
    """This method checks the available regions in holiday api with the given region"""
    try:
        endpoint = "AvailableCountries"
        response = api.get_response_from_api(endpoint)
        available_reg = [reg["countryCode"] for reg in response if response is not None]
        if code in available_reg:
            valid_code = code
        else:
            raise Exception
    except Exception as err:
        datas["logger"].error(
            "Cannot get the details from api for the given region %s %s", code, err
        )
        msg = f" {code} is not an available region in Holiday Api."
        valid_code = None
        # raise argparse.ArgumentTypeError(msg)
    return valid_code


def main():
    """This is the main method for this module"""
    parser = argparse.ArgumentParser(
        description="This argparser is to get the year, endpoints to fetch from holiday api"
    )
    parser.add_argument(
        "--fromyear",
        help="Enter the start year in format YYYY",
        type=valid_year,
        default=int(date.today().year),
    )
    parser.add_argument("--toyear", help="Enter the end year in format YYYY", type=valid_year)
    parser.add_argument(
        "--region", help="Enter the region code in alpha_2 format", type=valid_region, required=True
    )
    parser.add_argument(
        "endpoint",
        choices=["PublicHolidays", "LongWeekend", "nextPH"],
        help="Choose the endpoint",
    )
    args = parser.parse_args()
    api_details = PublicHolidaysLongWeekendS3(args.fromyear, args.toyear, args.region)
    if args.endpoint in ("PublicHolidays", "LongWeekend"):
        datas["logger"].info("Getting details from api for %s", args.endpoint)
        list_year = api_details.get_year_list_from_given_years()
        api_details.get_response_from_api(list_year, args.endpoint)
    else:
        response = api.get_endpoint_for_next_publicholidays(args.region)
        file_name = f"{args.endpoint}_for_{args.region}.json"
        partition_path = f"pt_region={args.region}"
        api_details.get_dataframe_for_response(
            response, None, file_name, partition_path, args.endpoint
        )


if __name__ == "__main__":
    main()
