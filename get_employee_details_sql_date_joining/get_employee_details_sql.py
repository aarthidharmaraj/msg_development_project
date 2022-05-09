"""This module is to get the employee details from sql by date_of_joining"""
import os
import configparser
from tkinter import YES
import pyodbc
import pandas as pd

parent_dir = os.path.dirname(os.getcwd())
config = configparser.ConfigParser()
config.read(parent_dir + "/details.ini")


class GetEmployeeFromSql:
    """This calss Gets the details of employee from sql"""

    def __init__(self, logger):
        """This is the init method for the class"""
        self.logger = logger
        self.driver = config["sql_server"]["driver"]
        self.host = config["sql_server"]["host"]
        self.database = config["sql_server"]["database"]
        self.user = config["sql_server"]["user"]
        self.password = config["sql_server"]["password"]

    def sql_connection(self):
        """This method is to connect the sql server using pyodbc"""
        # for driver in pyodbc.drivers():
        #     print(driver)
        try:
            conn = pyodbc.connect(
                Driver=self.driver,
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                trusted_connection=YES,
                autocommit=True,
            )
            self.logger.info("The connection has been established successfuly")
        except Exception as err:
            self.logger.info("Cannot connect to server")
            print("Cannot connect to the server", err)
            conn = None
        return conn

    def get_employee_details_by_joiningdate(self, startdate, enddate):
        """This method is to get the employee details from sql by filtering"""
        try:
            # query='SELECT * from employee where Date_of_Joining >= {} and
            # Date_of_Joining <= {}'.format(startdate, enddate)
            # query = "SELECT * FROM employee WHERE Date_of_Joining ="f"'{joining_date}'"
            conn = self.sql_connection()
            data = "SELECT * from employee where Date_of_Joining between ? and ?"
            df_data = pd.read_sql(data, conn, params=(startdate, enddate))
        except Exception as err:
            print("Cannot filter the employee details for given date", err)
            self.logger.error("Cannot filter the employee details for given date")
            df_data = None
        return df_data


# def main():
#     """This is the main method"""
#     logging.basicConfig(
#         level=logging.INFO,
#         filename="log_file.log",
#         datefmt="%d-%b-%y %H:%M:%S",
#         format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
#         filemode="w",
#     )
#     logger = logging.getLogger()
#     sql = GetEmployeeFromSql(logger)
#     # sql.sql_connection()
#     sql.get_employee_details_by_joiningdate("2021-05-31")


# if __name__ == "__main__":
#     main()
