"""This module get historic datas and products lists from eBird Api based on
region code and partition them based on date and upload to s3"""

from datetime import datetime,timedelta
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
    
    def __init__(self,start_date,end_date):
        """this is the init method for the class HistoricDataProductsUploadS3 """
        self.logger=datas['logger']
        self.startdate=start_date
        self.enddate=end_date
        self.region_code=ast.literal.eval(datas["config"]["eBird_api_datas"]["region_code"])
        self.s3_client = S3Details(self.logger, datas["config"])
        self.local_path = LoggerPath.local_download_path("historicdata_products_from_api")
        self.api=PullDataFromEBirdApi(self.logger,datas["config"])
        self.s3path = datas["config"]["eBird_api_datas"]["s3_path"]
        self.bucket = datas["config"]["eBird_api_datas"]["bucket"]
        self.last_run=datas["config"]["eBird_api_datas"]["last_run_date"]
        
        
    def get_employee_details_for_givendates(self):
        """This method is to get the employee details for the given dates"""
        if self.enddate:
            if self.startdate >(datetime.now().date()) or self.enddate >(datetime.now().date()):
                self.logger.info("Cannot fetch the details for future dates")
                sys.exit("script was terminated when fetching for future dates")
            else:
                if self.startdate < self.enddate:
                    dates = {"date1": self.startdate, "date2": self.enddate}
                    self.logger.info("Getting details from api for given startdate %s to enddate %s",self.startdate,self.enddate)
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
                        "Getting api datas between lastdate %s and current date %s",last_run,self.startdate
                    )
                dates = {"date1": self.startdate, "date2": self.startdate}
            elif self.startdate > (datetime.now().date()):
                    dates = {"date1": self.startdate, "date2": self.startdate}
                    self.logger.info("Getting datas from api for the current date alone")
                    print("Getting data only for current date")
            else:
                dates=None
                self.logger.info("Cannot fetch the details for future dates")
                sys.exit("script was terminated when fetching for future dates")   
        return dates
    
    def get_data_from_api_for_eachdate(self,date1,date2):
        """This method fetches historic data from api"""
        try:
            day_count = (date1 - date2).days + 1
            print(day_count)
            for day in range(day_count):
                single_date = date2 + timedelta(day)
                self.logger.info("Getting eBird api data for %s",single_date)
                for key,value in self.region_code.items():
                    response=self.api.fetch_historical_data_from_api(single_date,value)
        except Exception as err:
            self.logger.error("Cannot get the date %s",err)
            single_date=None
        return single_date
            
    def fetch_data_from_api_dataframe(self):
        """This method fetches data from api and convert to dataframe using pandas"""
       
            
        
    
    def put_partition_path(self, date):
        """This method will make partion path based on year,month,date
        and avoid overwrite of file and upload to local"""
        try:
            partition_path = date.strftime("pt_year=%Y/pt_month=%m/pt_day=%d")
            print(partition_path)
        except Exception as err:
            self.logger.error("Cannot made a parttiion %s",err)
            print("Cannot made a partition because of this error", err)
            partition_path = None
        return partition_path

    def upload_file_to_s3(self, source, bucket_path, partition_path, file_name):
        """This method used to upload the file to s3 in the partiton created"""
        try:
            key = partition_path + "/" + file_name
            self.logger.info(
                "The file %s being uploaded to s3 in the given path %s", file_name, key
            )
            print("the file has been uploaded to s3 in the given path")
            file = self.s3_client.upload_file(source, self.bucket, bucket_path, key)
            os.remove(source)
        except Exception as err:
            self.logger.error("Cannot upload the files to s3 in the given path %s", err)
            file = None
        return file
    
    def last_date_of_execution(self):
        """If there is no end date,this method get last date of api run
        and upadates it to the config file"""
        if not self.enddate:
            # if not end_date:
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
        date= datetime.strptime(date, "%Y-%m-%d").date()
        datas["logger"].info(f"The given date is a valid date {date}")
    except ValueError:
        datas["logger"].info(f"{date} is not a valid date.It must be in format YYYY-MM-DD")
        msg =f" {date} is not a valid year.It must be in format YYYY"
        date=None
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
        default=datetime.now().date()
    )
    parser.add_argument(
        "--enddate",
        help="The date should be in - format YYYY-MM-DD",
        type=valid_date,
    )
    args = parser.parse_args()
    
    api_data=HistoricDataProductsUploadS3(args.startdate, args.enddate)
    dates=api_data.get_employee_details_for_givendates()
    api_data.get_data_from_api_for_eachdate(dates["date1"],dates["date2"])
    api_data.last_date_of_execution()
    
if __name__ == "__main__":
    main()
