"""This module is to get the employee details from sql by date_of_joining"""
import os
import configparser
from tkinter import YES
import pyodbc
import pandas as pd
from sqlalchemy.engine import URL, create_engine

parent_dir = os.path.dirname(os.getcwd())
config = configparser.ConfigParser()
config.read(parent_dir + "/details.ini")


class GetEmployeeFromSql:
    """This calss Gets the details of employee from sql"""

    def __init__(self, logger):
        """This is the init method for the class"""
        self.logger = logger

    def sql_connection(self, driver, host, database, user, password):
        """This method is to connect the sql server using pyodbc"""
        # for drivers in pyodbc.drivers():
        #     print(drivers)
        try:
            conn = pyodbc.connect(
                Driver=driver,
                host=host,
                database=database,
                user=user,
                password=password,
                trusted_connection=YES,
                autocommit=True,
            )
            # connection_string="DRIVER={ODBC Driver 18 for SQL Server};host=localhost;DATABASE=Employee_payroll;UID=root;PWD=Aspire@123"
            # connection_string="Driver={driver};Server='{host}';Database='{database}';User='{user}';Password='{password}';trusted_connection='yes';auto_commit='True'"
            # connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
            # conn = create_engine(connection_url)
            self.logger.info("The connection has been established successfuly")
        except Exception as err:
            self.logger.info("Cannot connect to server")
            print("Cannot connect to the server", err)
            conn = None
        return conn

    def get_employee_details_by_joiningdate(self, driver, host, database, user, password, query):
        """This method is to get the employee details from sql by filtering"""
        try:
            conn = self.sql_connection(driver, host, database, user, password)
            # chunk_data=[]
            # for df_data in pd.read_sql(query, conn,chunksize=2):
            #     chunk_data.append(df_data)
            # data=pd.concat(chunk_data)
            chunk_data = pd.read_sql(query, conn, chunksize=1000)
            df_data = pd.concat(chunk_data)
        except Exception as err:
            print("Cannot filter the employee details for given date", err)
            self.logger.error("Cannot filter the employee details for given date")
            df_data = None
        return df_data

    def get_query_from_where_condition(self, start, condition, end, table, column):
        """This method gets the query from where condition"""
        # end=None
        try:
            if end is not None and condition.lower() == "between":
                query = f"SELECT * from {table} where {column} {condition} '{start}'and '{end}' Order By {column} asc"
            elif end is None and condition.lower() != "between":
                query = f"SELECT * FROM {table}  WHERE {column} {condition} '{start}' Order By {column} asc"
            else:
                print("Cannot get the data for the given condition", condition)
                self.logger.info("Cannot get the data for the given condition")
                query = None
        except Exception as err:
            print("Cannot get the query", err)
            self.logger.info("Cannot execute the query")
            query = None
        return query

    def get_datas_from_sql(self, start, condition, end):
        """This method gets required datas from config for connection"""
        driver = config["sql_server"]["driver"]
        host = config["sql_server"]["host"]
        database = config["sql_server"]["database"]
        user = config["sql_server"]["user"]
        password = config["sql_server"]["password"]
        table = config["sql_server"]["table"]
        column = config["sql_server"]["column"]
        try:
            get_query = self.get_query_from_where_condition(start, condition, end, table, column)
            print(get_query)
            response = self.get_employee_details_by_joiningdate(
                driver, host, database, user, password, get_query
            )
        except Exception:
            self.logger.info("Cannot get the response for given dates")
            response = None
        return response
