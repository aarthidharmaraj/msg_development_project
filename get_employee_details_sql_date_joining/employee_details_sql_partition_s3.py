""" This module is to get the details of employees as per their date of 
joining and upload to s3 in that partition of joining date"""


import logging
import configparser
import os
import argparse
from datetime import datetime, timedelta
import sys
from time import time
from get_employee_details_sql import GetEmployeeFromSql
from employee_details_upload_local import EmployeeDetailsPartitionLocalUpload

from aws_s3.s3_details import S3Details

parent_dir = os.path.dirname(os.getcwd())
config = configparser.ConfigParser()
config.read(parent_dir + "/details.ini")


class EmployeeDetailsJoiningDate:
    """This class gets employees details based on joiningdate and upload to s3"""

    def __init__(self, logger_obj, startdate, enddate, condition):
        """This is the init method of the class EmployeeDetailsJoiningDate"""
        self.logger = logger_obj
        self.startdate = startdate
        self.enddate = enddate
        self.condition = condition
        self.local_sqlpath = os.path.join(
            parent_dir, config["local"]["data_path"], "sql_employee_details"
        )
        if not os.path.exists(self.local_sqlpath):
            os.makedirs(self.local_sqlpath)
        self.local = EmployeeDetailsPartitionLocalUpload(logger_obj)
        self.sql = GetEmployeeFromSql(logger_obj)
        self.s3_client = S3Details(logger_obj)

    def get_employee_details_for_givendates(self):
        """This method is to get the employee details for the given dates"""
        if self.enddate <= (datetime.now().date()):
            if self.startdate < self.enddate:
                dates = {"date1": self.startdate, "date2": self.enddate}
            else:
                self.logger.info("Cannot fetch details as startdate is greater than enddate")
                print("Cannot fetch the details between", self.startdate, "and", self.enddate)
                sys.exit("script was terminated since startdate is greater than enddate")
            self.logger.info("Filtering Employee details from given startdate to enddate")
            print("Filtering Employee details from given startdate to enddate")
        else:
            self.logger.info("Cannot fetch the details for future dates")
            print("Cannot fetch the details for future dates")
            sys.exit("script was terminated when fetching for future dates")
        return dates

    def employee_details_from_sql(self):
        """This method gets the employee details from sql and filter based on dates"""
        dates = self.get_employee_details_for_givendates()
        response = self.sql.get_query_from_where_condition(
            dates["date1"], self.condition, dates["date2"]
        )
        print(response)
        if response is not None:
            day_count = (dates["date2"] - dates["date1"]).days + 1
            for day in range(day_count):
                single_date = dates["date1"] + timedelta(day)
                # print(single_date)
                self.limit_dataframe_from_response(response, single_date)
        else:
            print("Cannot fetch the data from sql due to problem in sql ")
            self.logger.error("Cannot fetch the data from sql due to problem in sql")
            sys.exit("The script has terminated due to problem in sql")

    def limit_dataframe_from_response(self, response, single_date):
        """This method limits the dataframe with row limit and filter them"""
        index = int(config["sql_employee_details_joiningdate"]["index"])
        limit = int(config["sql_employee_details_joiningdate"]["row_limit"])
        while index < len(response):
            limit_df = response.iloc[index : index + limit]
            self.filter_create_response_by_date(limit_df, single_date)
            self.logger.info("Filtering the response based on dates")
            index = index + limit

    def filter_create_response_by_date(self, df_data, date):
        """This method filters and creates a newdataframe from the response
        based on date using pandas"""
        epoch = int(time())
        file_name = f"employee_{epoch}.json"
        try:
            new_df = df_data[(df_data["Date_of_Joining"] == date)]
            if not new_df.empty:
                new_df_str = new_df.astype({"Date_of_Joining": str})
                print(new_df_str)
                new_df_str.to_json(
                    self.local_sqlpath + "/" + file_name, orient="records", lines=True
                )
                partition_path = self.put_partition_path(date)
                copy_source = self.local_sqlpath + "/" + file_name
                self.local.upload_parition_s3_local(
                    self.local_sqlpath, copy_source, file_name, partition_path
                )
                # self.upload_to_s3(copy_source,partition_path, file_name)
            return True
        except Exception as err:
            print("Cannot filter the datas in dataframe due to this error:", err)
            self.logger.error("Cannot filter the datas in dataframe due to an error")
            return False

    def put_partition_path(self, date):
        """This method will make partion path based on year,month,date
        and avoid overwrite of file and upload to local"""
        try:
            partition_path = date.strftime("pt_year=%Y/pt_month=%m/pt_day=%d")
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
    log_dir = os.path.join(parent_dir, config["local"]["log_path"], "sql_employee_log")
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
    parser.add_argument(
        "--condition",
        help="Condition to use in sql query",
        default=config["sql_employee_details_joiningdate"]["where_condition"],
    )
    args = parser.parse_args()
    employee_details = EmployeeDetailsJoiningDate(
        logger_obj, args.startdate, args.enddate, args.condition
    )
    employee_details.employee_details_from_sql()


if __name__ == "__main__":
    main()
