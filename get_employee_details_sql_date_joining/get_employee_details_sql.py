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
        self.table = config["sql_server"]["table"]
        self.user = config["sql_server"]["user"]
        self.password = config["sql_server"]["password"]

    def sql_connection(self):
        """This method is to connect the sql server using pyodbc"""
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

    def get_employee_details_by_joiningdate(self, query):
        """This method is to get the employee details from sql by filtering"""
        try:
            conn = self.sql_connection()
            # query = "SELECT * from employee where Date_of_Joining between ?
            # and ? Order By Date_of_Joining asc"
            # df_data=pd.read_sql(query, conn, params=(startdate, enddate))
            df_data = pd.read_sql(query, conn)
        except Exception as err:
            print("Cannot filter the employee details for given date", err)
            self.logger.error("Cannot filter the employee details for given date")
            df_data = None
        return df_data

    def get_query_from_where_condition(self, start, condition, end):
        """This method gets the query from where condition"""
        table = config["sql_server"]["table"]
        column = config["sql_server"]["column"]
        # end=None
        try:
            if end is not None and condition.lower() == "between":
                query = f"SELECT * from {table} where {column} {condition} '{start}'and '{end}' Order By {column} asc"
                response = self.get_employee_details_by_joiningdate(query)
            elif end is None and condition.lower() != "between":
                query = f"SELECT * FROM {table}  WHERE {column} {condition} '{start}' Order By {column} asc"
                response = self.get_employee_details_by_joiningdate(query)
            else:
                print("Cannot get the data for the given condition", condition)
                self.logger.info("Cannot get the data for the given condition")
                response = None
        except Exception as err:
            print("Cannot get the query", err)
            self.logger.info("Cannot execute the query")
            response = None
        return response
