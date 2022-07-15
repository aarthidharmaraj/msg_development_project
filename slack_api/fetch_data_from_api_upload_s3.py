"""This module is to get data from Api for admin analytics data for the given date,
decompress the compressed json response and upload them to s3 in the partition path 
based on year, month and date"""

import sys
import argparse
import os
import gzip
from datetime import datetime,timedelta
from logger_path.logger_object_path import LoggerPath
from aws_s3.s3_details import S3Details
from get_response_from_api import SlackApi

datas = LoggerPath.logger_object("slack_api")

class FetchAdminAnalyticsData:
    """This class has methods to get admin analytics data from Api, decompress the json
    response and upload to s3"""
    
    def __init__(self,start_date,end_date):
        """This is the init method for the class FetchAdminanalyticsData"""
        self.section = datas["config"]["sunrise_sunset_api"]
        self.logger = datas["logger"]
        self.startdate = start_date
        self.enddate = end_date
        
    def get_details_for_givendates(self):
        """This method is to get the datas from slack Api for the given range of dates"""
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
            else:
                dates = {"date1": self.startdate, "date2": self.startdate}
                self.logger.info(
                    "Fetching datas from api between %s and current date %s",
                    self.startdate,
                    self.startdate,
                )
        except Exception as err:
            dates = None
            self.logger.info("Cannot get the details for the given dates %s", err)
            print("script was terminated when fetching for given dates")
        return dates
    
    def get_admin_analytics_from_api(self, date1, date2):
        """This method fetches the admin analytics data from api for the dates given 
        parameters : date1 ,date 2 - the dates on which datas are needed"""
        try:
            api = SlackApi(self.section, self.logger)
            day_count = (date1 - date2).days + 1
            for day in range(day_count):  # Fetch for the given dates one by one
                single_date = date2 + timedelta(day)
                self.logger.info("Getting response from api for %s date", single_date)
                response = api.get_endpoint_for_admin_analytics_data(single_date)
                self.decompress_json_response(response, single_date)
        except Exception as err:
            self.logger.error("Cannot able to get response from api for the date- %s", err)
            response = None
            sys.exit("System has terminated for fail in getting response from api")
        return response
    
    def decompress_json_response(self,response,date):
        """This method is to get the decompressed json file from the response (compressed json)"""
        try:
            temp_s3_path=LoggerPath.local_download_path("slack_api_data")
            s3_path = self.section["s3_path"]
            #response_format="Member Analytics 2020-08-01.json.gz"
            decomp=gzip.GzipFile(response,"rb") #decompress
            decompressed_data =decomp.read()
            copy_source = temp_s3_path + "/" + "decompressed_data.json"
            self.logger.info("Successfully decompressed the json from the response")
            with open(copy_source) as file:
                file.write(decompressed_data)
            self.logger.info("Successfully created json file with decompressed data in the temporary s3 path")
            partition_path=self.put_partition_path(date)
            file_name=f"Member_analytics_data_on_{date}.json"
            file=self.upload_file_to_s3(s3_path,copy_source,partition_path,file_name)
            
        except Exception as err:
            self.logger.error("Cannot decompress the response %s",err)
            file=None
        return file
    
    def put_partition_path(self, date):
        """This method will make partion path based on year,month and date
        and avoid overwrite of file and upload to local"""
        try:
            partition_path = date.strftime(f"pt_year=%Y/pt_month=%m/pt_day=%d")
            self.logger.info("Created the partition path for the given %s", date)
        except Exception as err:
            self.logger.error("Cannot made a partition for %s because of %s", date, err)
            partition_path = None
        return partition_path

    def upload_file_to_s3(self, bucket_path, source, partition_path, file_name):
        """This method used to upload the file to s3 in the partiton created
        parameters: file_name - name of the file to be uploaded
                    source - the source which has the file and its data
                    partition_path - path has partition based on endpoint and date"""
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

def check_valid_date(given_date):
    """This method checks for the valid date entered and returns in datetime.date() format"""
    try:
        given_date = datetime.strptime(given_date, "%Y-%m-%d").date()
        date_limit=datetime.strptime("2020-01-01","%Y-%m-%d").date() #Api has datas after 1st Jann 2020
        if given_date < datetime.now().date() and given_date > date_limit:  # checks with current date
            valid_date = given_date
            datas["logger"].info("The given date is a valid date %s", valid_date)
        else:
            datas["logger"].info("Cannot get datas from api for uncompleted days and unavailable dates in Api")
            raise Exception
    except (Exception, ValueError):
        datas["logger"].info(
            "%s not valid date.It must be in format YYYY-MM-DD and must be ended dates and available dates of Api",
            given_date,
        )
        valid_date = None
        msg = f"{given_date} not valid.It must be in format YYYY-MM-DD and must be ended dates and available dates in Api"
        raise argparse.ArgumentTypeError(msg)
    return valid_date

def main():
    """This is the main method for this module"""
    parser = argparse.ArgumentParser(
        description="This argparser gets start,end dates to get data from slack api"
    )
    parser.add_argument(
        "--startdate",
        help="The date should be in format YYYY-MM-DD",
        type=check_valid_date,
        default=datetime.now().date()-timedelta(days=1),
    )
    parser.add_argument(
        "--enddate",
        help="The date should be in format YYYY-MM-DD",
        type=check_valid_date,
    )
    args = parser.parse_args()
    api=FetchAdminAnalyticsData(args.startdate,args.enddate)
    dates = api.get_details_for_givendates()
    api.get_admin_analytics_from_api(dates["date1"],dates["date2"])

if __name__ == "__main__":
    main()
