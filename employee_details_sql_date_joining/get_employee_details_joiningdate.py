"""This module is to get the details of employees as per their date of 
joining and upload to s3 in that parttition of joining date"""

import mysql.connector
import logging
import configparser
import os
import argparse
from datetime import datetime
import pandas as pd

parent_dir = os.path.dirname(os.getcwd())
config = configparser.ConfigParser()
config.read(parent_dir + "/details.ini")

class EmployeeDetailsJoiningDate:
    """This class gets employees details based on joiningdate and upload to s3"""
    def __init__(self,logger_obj,mydb,date):
        """This is the init method of the class EmployeeDetailsJoiningDate"""
        self.logger=logger_obj
        self.mydb=mydb
        self.joining_date=date
        
    
    def get_employee_details_by_joiningdate(self):
        """This method is to get the employee details from sql by filtering"""
        try:
            # df = pd.read_sql(('select "Date_of_Joining","emp_id" from "employee" '
            #             'where "Date_of_Joining" = %(date)s'),
                        # params={"date":self.joining_date},con=self.mydb,index_col='Date_of_Joining')
            query="SELECT * FROM employee WHERE Date_of_Joining = {date}".format(date=self.joining_date);
            cursor=self.mydb.cursor()
            re=cursor.execute(query)
            print(re)
        except Exception as err:
            print("Cannot filter the employee details for given date",err)
            self.logger.error("Cannot filter the employee details for given date")
        
def valid_date(date):
    """This method checks for the valid date entered and returns in datetime.date() format"""
    try:
        return datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError:
        msg = "not a valid date: {0!r}".format(date)
        raise argparse.ArgumentTypeError(msg)
    
def main():
    """This is the main method for this module"""
    log_dir = os.path.join(parent_dir, config["log_sql_employee"]["log_path"])
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
    mydb=mysql.connector.connect(host="localhost",user="***",passwd="***",database='Employee_payroll')
    parser = argparse.ArgumentParser(
        description="This argparser is to get the required date_of_joining need to filter"
    )
    parser.add_argument(
        "--date",
        help="The Joining Date - format YYYY-MM-DD",
        type=valid_date,
        default=datetime.now().date()
    )
    args = parser.parse_args()
    employee_details=EmployeeDetailsJoiningDate(logger_obj,mydb,args.date)
    employee_details.get_employee_details_by_joiningdate()
    
if __name__ == "__main__":
    main()
 
    