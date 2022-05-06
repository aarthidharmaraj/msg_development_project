"""This module is to get the details of employees as per their date of 
joining and upload to s3 in that parttition of joining date"""


import logging
import configparser
import os
import argparse
from datetime import datetime, timedelta
import sys
from time import time
import pandas as pd
from get_employee_details_sql import GetEmployeeFromSql
from employee_details_upload_local import EmployeeDetailsPartitionLocalUpload

# from aws_s3.s3_details import S3Details

parent_dir = os.path.dirname(os.getcwd())
config = configparser.ConfigParser()
config.read(parent_dir + "/details.ini")


class EmployeeDetailsJoiningDate:
    """This class gets employees details based on joiningdate and upload to s3"""

    def __init__(self, logger_obj, startdate, enddate):
        """This is the init method of the class EmployeeDetailsJoiningDate"""
        self.logger = logger_obj
        self.startdate = startdate
        self.enddate = enddate
        self.local_sqlpath = os.path.join(
            parent_dir, config["sql_employee_details_joiningdate"]["local_file_path"]
        )
        self.local = EmployeeDetailsPartitionLocalUpload(logger_obj)
        self.sql = GetEmployeeFromSql(logger_obj)
        # self.s3_client = S3Details(logger_obj)

    def get_employee_details_for_givendates(self):
        """This method is to get the employee details for the given dates"""
        if self.startdate < self.enddate:
            dates = {"date1": self.enddate, "date2": self.startdate}
        else:
            dates = {"date1": self.startdate, "date2": self.enddate}
        self.logger.info("Filtering Employee details from given startdate to enddate")
        print("Filtering Employee details from given startdate to enddate")
        return dates

    def get_employeedetails_for_each_date(self, date1, date2):
        """This method gets employee details between the two dates onebyone"""
        day_count = (date1 - date2).days + 1
        print(day_count)
        for day in range(day_count):
            single_date = date2 + timedelta(day)
            self.logger.info("Getting metaweather data for provided dates one by one")
            self.get_employee_details_from_sql(single_date)

    def get_employee_details_from_sql(self, date):
        """This method gets the employee detailsfor given date from sql"""
        response = self.sql.get_employee_details_by_joiningdate(date)
        if response is not None:
            self.filter_group_response_by_date(response, date)
        else:
            print("Cannot fetch the data from api due to problem in api ")
            self.logger.error("Cannot fetch the data from api due to problem in api")
            sys.exit("The script has terminated due to problem in sql")

    def filter_group_response_by_date(self, response, emp_name, date):
        """This method filters and creates a dataframe from the response
        based on date using pandas"""
        epoch = int(time())
        file_name = f"metaweather_{emp_name}_{epoch}.json"
        df_data = pd.DataFrame(response)

        try:
            new_df = df_data[(df_data.created.str.contains(emp_name))]
            # if not new_df.empty:
            #     filter_time = date
            new_df.to_json(self.local_sqlpath + "/" + file_name, orient="records", lines=True)
            partition_path = self.put_partition_path(emp_name, date)
            copy_source = self.local_sqlpath + "/" + file_name
            self.local.upload_parition_s3_local(copy_source, file_name, partition_path)
            # self.upload_to_s3(copy_source,partition_path, file_name)
            # return True
        except Exception as err:
            print("Cannot filter the datas in dataframe due to this error:", err)
            self.logger.error("Cannot filter the datas in dataframe due to an error")
            # return False

    def put_partition_path(self, date):
        """This method will make partion path based on city,year,month,date
        and hour and avoid overwrite of file and upload to local"""
        try:
            date = datetime.strptime(date, "%Y-%m-%d")
            partition_path = date.strftime(f"pt_year=%Y/pt_month=%m/pt_day=%d/pt_hour=%H/")
            print(partition_path)
        except Exception as err:
            self.logger.error("Cannot made a parttiion")
            print("Cannot made a partition because of this error", err)
            partition_path = None
        return partition_path

    def upload_to_s3(self, source, partition_path, file_name):
        """This method used to upload the file to s3 in the partiton created"""
        key = partition_path + "/" + file_name
        self.logger.info("The file is being uploaded to s3 in the given path")
        print("the file has been uploaded to s3 in the given path")
        self.s3_client.upload_file(source, key)


def valid_date(date):
    """This method checks for the valid date entered and returns in datetime.date() format"""
    try:
        return datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError:
        msg = "not a valid date: {0!r}".format(date)
        raise argparse.ArgumentTypeError(msg)


def main():
    """This is the main method for this module"""
    log_dir = os.path.join(parent_dir, config["sql_employee_details_joiningdate"]["log_path"])
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_file = os.path.join(log_dir, "sqlemployee_logfile.log")
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
        "--startdate",
        help="The date should be in format YYYY-MM-DD",
        type=valid_date,
        default=datetime.now().date() - timedelta(days=1),
    )
    parser.add_argument(
        "--enddate",
        help="The date should be in - format YYYY-MM-DD",
        type=valid_date,
        default=datetime.now().date(),
    )
    args = parser.parse_args()
    employee_details = EmployeeDetailsJoiningDate(logger_obj, args.startdate, args.enddate)
    dates = employee_details.get_employee_details_for_givendates()
    # employee_details.get_employeedetails_for_a_date(dates["date1"], dates["date2"])
    employee_details.get_employee_details_from_sql(dates["date1"], dates["date2"])


if __name__ == "__main__":
    main()
