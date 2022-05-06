"""This module is to get the employee details from sql by date_of_joining"""
import os
import configparser
import logging
from datetime import datetime
from tkinter import YES
import pyodbc
import pandas as pd
from sqlalchemy.engine import URL

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
        for driver in pyodbc.drivers():
            print(driver)
        try:
            conn = pyodbc.connect(
                driver=self.driver,
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                trusted_connection=YES,
                autocommit=True,
            )
            self.logger.info("The connection has been established successfuly")
            # cursor.execute('SELECT * FROM employee')
            # rows = cursor.fetchall()
            # for data in rows:
            #     print(data)
            connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": conn})

            engine = create_engine(connection_url)

        except Exception as err:
            self.logger.info("Cannot connect to server")
            print("Cannot connect to the server", err)
            engine = None
        return engine

    def get_employee_details_by_joiningdate(self, joining_date):
        """This method is to get the employee details from sql by filtering"""
        try:
            # df = pd.read_sql(('select * from "employee" '
            #             'where "Date_of_Joining" = %(startdate)s between "Date_of_joining"=%(enddate)s'),
            #             params={"startdate":startdate,"endadte":enddate},con=mydb,index_col='Date_of_Joining')
            date_time = datetime.strptime(joining_date, "%Y-%m-%d").date()
            query = "SELECT * FROM employee WHERE Date_of_Joining ={date}".format(date=date_time)
            # cursor.execute(query)
            # rows=cursor.fetchall()
            # print(rows)
            # for data in rows:
            #     print(data)
            conn = self.sql_connection()
            sql_data = pd.read_sql(query, conn)
            # conn.close()
            print(sql_data)
        except Exception as err:
            print("Cannot filter the employee details for given date", err)
            self.logger.error("Cannot filter the employee details for given date")


def main():
    """This is the main method"""
    logging.basicConfig(
        level=logging.INFO,
        filename="log_file.log",
        datefmt="%d-%b-%y %H:%M:%S",
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
        filemode="w",
    )
    logger = logging.getLogger()
    sql = GetEmployeeFromSql(logger)
    # sql.sql_connection()
    sql.get_employee_details_by_joiningdate("2021-05-31")


if __name__ == "__main__":
    main()
