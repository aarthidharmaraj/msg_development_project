"""This method gets the Public holidays, Long weekend details from api based on the given year and Country which are available in api
partition them based on year,range month and upload to s3"""
import os
import argparse
from datetime import date
import sys
import pandas as pd
from logger_path.logger_object_path import LoggerPath
from aws_s3.s3_details import S3Details
from fetch_data_partition_upload_local import ApiDataPartitionUploadLocal
from get_response_from_holidayapi import HolidayApi

datas = LoggerPath.logger_object("holiday_api_data")

class PublicHolidaysLongWeekendS3:
    """This method fetches the public holidays and Long weekend data from holiday api, partition them and upload to s3 """
    def __init__(self,startyear,endyear,endpoint,region):
        """This is the init method for the class PublicHolidaysLongWeekendS3"""
        self.startyear=startyear
        self.endyear=endyear
        self.endpoint=endpoint
        self.region=region
        self.section=datas["config"]["holiday_api"]
        self.logger=datas["logger"]
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
            self.logger.info(f"Created a list for the given years{years_list}")
        except Exception as err:
            self.logger.error(f"Cannot get the list of years {err}")
            print(err)
            years_list = None
        return years_list
    
    def get_response_from_api(self, years_list):
        """This method gets the nobel prizes and laureates response from api
        and convert to a dataframe using pandas"""
        try:
            for year in years_list:
                self.logger.info(
                    f"Getting response from api for {self.endpoint} for the year {year}"
                )
                if self.endpoint=="PublicHolidays" or self.endpoint=="LongWeekend":
                    response = self.api.get_endpoint_for_public_holidays_long_weekend(year,self.region,self.endpoint)
                else:
                    response=self.api.get_endpoint_for_next_publicholidays(year,self.region)
                print(response)
                df_data=None
                # if response is not None:
                #     self.logger.info(f"Got the response from api for the year {year}")
                #     df_data = pd.DataFrame(response[self.endpoint])
                #     if not df_data.empty:
                #         self.create_json_file_partition(df_data, year)
                # else:
                #     self.logger.info(f"No responses from api {response} for the year {year}")
                #     df_data = None
        except Exception as err:
            self.logger.error(f"Cannot able to get response from api for the year{year}, {err}")
            df_data = None
            sys.exit(
                "System has terminated for fail in getting public holidays or long weekend response from api"
            )
        return df_data

    def create_json_file_partition(self, df_data, award_year):
        """This method creates a temporary json file,create partition path and upload to s3"""
        try:
            file_name = f"{self.endpoint}_{award_year}.json"
            df_data.to_json(
                self.local_path + "/" + file_name,
                orient="records",
                lines=True,
            )
            self.logger.info(f"Created the json file for {award_year} ")
            partition_path = self.put_partition_path(award_year)
            copy_source = self.local_path + "/" + file_name
            file_name = self.local.upload_parition_s3_local(
                self.local_path,
                copy_source,
                file_name,
                partition_path,
            )
            # file_name=self.upload_to_s3(copy_source,partition_path, file_name)
        except Exception as err:
            self.logger.error(f"Cannot create a json file for{file_name}")
            print("Canot create a json file", err)
            file_name = None
        return file_name

    def put_partition_path(self, award_year):
        """This method will make partion path based on year
        and avoid overwrite of file and upload to local"""
        try:
            partition_path = f"{self.partition_path}/{self.endpoint}/pt_year={award_year}"
            print(partition_path)
            self.logger.info(
                f"Created the partition path for the given {self.endpoint} and {award_year}"
            )
        except Exception as err:
            self.logger.error(f"Cannot made a partition becausee of {err}")
            print(
                "Cannot made a partition because of this error",
                err,
            )
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
            datas["logger"].info(f"The given year is a valid year {year}")
        else:
            raise ValueError
    except ValueError:
        datas["logger"].info(f"{year} is not a valid year.It must be in format YYYY")
        msg = f" {year} is not a valid year.It must be in format YYYY"
        year_valid = year
        raise argparse.ArgumentTypeError(msg)
    return year_valid

def valid_region(code):
    """This method checks the available regions in holiday api with the given region"""
    try:
        endpoint="AvailableCountries"
        api = HolidayApi(datas["config"]["holiday_api"],datas["logger"])
        response=api.get_response_from_api(endpoint)
        available_reg=[reg["countryCode"] for reg in response if response is not None]
        if code in available_reg:
            valid_code=code
        else:
            raise Exception
    except Exception as err:
        datas["logger"].error("Cannot get the details from api for the given region %s %s",code,err)
        msg = f" {code} is not an available region in Holiday Api."
        valid_code = None
        raise argparse.ArgumentTypeError(msg)
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
    parser.add_argument("--region", help="Enter the region code in alpha_2 format", type=valid_region,required=True)
    parser.add_argument(
        "endpoint",
        choices=["PublicHolidays", "LongWeekend","nextPH"],
        help="Choose the endpoint",
    )
    args = parser.parse_args()
    api_details =PublicHolidaysLongWeekendS3(
        args.fromyear,
        args.toyear,
        args.endpoint,
        args.region
    )
    list_year = api_details.get_year_list_from_given_years()
    api_details.get_response_from_api(list_year)


if __name__ == "__main__":
    main()
