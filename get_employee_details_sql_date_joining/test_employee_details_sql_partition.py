"""Thi module tests for getting employee details from sql, and partition 
    them and upload to s3 and local"""
import os
import logging
import configparser
from datetime import datetime
from time import time
import pytest
import pandas as pd
from employee_details_sql_partition_s3 import EmployeeDetailsPartitionS3, valid_date
from employee_details_upload_local import EmployeeDetailsPartitionLocalUpload
from get_employee_details_sql import EmployeeFromSql
from aws_s3.s3_details import S3Details

parent_dir = os.path.dirname(os.getcwd())
config = configparser.ConfigParser()
config.read(parent_dir + "/details.ini")

log_dir = os.path.join(parent_dir, config["local"]["log_path"], "sql_employee_log")
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
log_file = os.path.join(log_dir, "sqlemployee_logfile.log")

logging.basicConfig(
    level=logging.INFO,
    filename="sqlemployee_logfile.log",
    datefmt="%d-%b-%y %H:%M:%S",
    format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    filemode="w",
)
logger_obj = logging.getLogger()


@pytest.fixture
def start_date():
    """This method returns the start_date"""
    startdate = "2022-05-08"
    return datetime.strptime(startdate, "%Y-%m-%d").date()


@pytest.fixture
def end_date():
    """This method returns the end_date"""
    enddate = "2022-05-10"
    return datetime.strptime(enddate, "%Y-%m-%d").date()

@pytest.fixture
def date_none():
    """This method returns the date as none"""
    return None

@pytest.fixture
def file_name():
    """This method returns the file name in epoch format"""
    return f"employee_{int(time())}.json"


@pytest.fixture
def partition():
    """This method returns the partition path"""
    partition_path = "pt_year=2022/pt_month=05/pt_day=06/"
    return partition_path


@pytest.fixture
def sqlpath():
    """This method returns the sql path"""
    sqlpath = os.path.join(parent_dir, config["local"]["data_path"], "sql_employee_details")
    if not os.path.exists(sqlpath):
        os.makedirs(sqlpath)
    return sqlpath


@pytest.fixture
def copy_source(sqlpath, file_name):
    """This method returns the copy source to upload in local"""
    source = sqlpath + "/" + file_name
    return source


@pytest.fixture
def condition():
    """This method returns the condition between for query"""
    return "between"


@pytest.fixture
def condition_not_between():
    """This method returns the condition other than between for query"""
    return "="


@pytest.fixture
def response():
    """This method returns the resposne"""
    data = {"Name": ["emp1", "emp2"], "Date_of_Joining": ["2022-05-08", "2022-05-09"]}
    return pd.DataFrame(data)


@pytest.fixture
def sql_server():
    """This method returns the server credentials to connect to server"""
    driver = "{ODBC Driver 17 for SQL Server}"
    host = "localhost"
    user = "SA"
    password = "Aspire@123"
    database = "Employee_payroll"
    client = {
        "driver": driver,
        "host": host,
        "user": user,
        "password": password,
        "database": database,
    }
    return client


@pytest.fixture
def table():
    """This method returns the table for fetching datas"""
    return "employee"


@pytest.fixture
def column():
    """This method returns the colummn to fetch required datas"""
    return "Date_of_Joining"

@pytest.fixture
def false_table():
    """This method returns the table for fetching datas"""
    return "employeess"


@pytest.fixture
def false_column():
    """This method returns the colummn to fetch required datas"""
    return "joining_date"

def sql_server_fail():
    """This method returns the server credentials for fail connect to server"""
    driver = "{MySQL ODBC 8.0 Unicode Driver}"
    host = "localhost"
    user = "user"
    password = "pass"
    database = "Employee_payroll"
    client = {
        "driver": driver,
        "host": host,
        "user": user,
        "password": password,
        "database": database,
    }
    return client


@pytest.fixture
def query(table, column, condition, start_date, end_date):
    """This method returns the query to read from sql"""
    query = f"SELECT * from {table} where {column} {condition} '{start_date}'and '{end_date}'"
    return query

@pytest.fixture
def query_fail(false_table,false_column,condition,start_date,end_date):
    """This method returns the false query to check"""
    query = f"SELECT * from {false_table} where {false_column} {condition} '{start_date}'and '{end_date}'"
    return query

class TestS3Details:
    """This Test Class will check for all the possible
    Test cases in S3Operations"""

    def test_s3operations_objects(self):
        """This Method will test for the instance belong to the class S3Operations"""
        self.obj = S3Details(logger_obj)
        assert isinstance(self.obj, S3Details)

    def test_upload_s3_passed(self):
        """This tests for put object in S3 class S3details"""
        self.s3_client = S3Details(logger_obj)
        res = self.s3_client.upload_file("flow of code", "flow.txt")
        assert res == "flow of code"

    @pytest.mark.xfail
    def test_upload_s3_failed(self):
        """This method will test for the report is uploaded to S3 in class S3Operations"""
        self.s3_client = S3Details(logger_obj)
        res = self.s3_client.upload_file(b"flow of code", "flow.txt")
        assert res is None


class TestUploadPartitionLocal:
    """This class has methods to test possible testcases in
    employee details upload local module"""

    def test_parition_local_object(self):
        """This method tests the instance of class MetaweatherPartitionlocal"""
        self.obj_local = EmployeeDetailsPartitionLocalUpload(logger_obj)
        assert isinstance(self.obj_local, EmployeeDetailsPartitionLocalUpload)

    def test_write_data_to_local_file_is_done(self, sqlpath, copy_source, file_name, partition):
        """This method will test for the data is written to local file
        in class EmployeeDetailsPartitionLocalUpload"""
        self.obj_local = EmployeeDetailsPartitionLocalUpload(logger_obj)
        new_df = pd.DataFrame()
        new_df.to_json(sqlpath + "/" + file_name, orient="records", lines=True)
        local_file = self.obj_local.upload_parition_s3_local(
            sqlpath, copy_source, file_name, partition
        )
        assert local_file == f"employee_{int(time())}.json"

    @pytest.mark.xfail
    def test_write_data_to_local_file_is_not_done(self, file_name, partition):
        """This method will test for the data is not written
        to local file in class  EmployeeDetailsPartitionLocalUpload"""
        self.obj_local = EmployeeDetailsPartitionLocalUpload(logger_obj)
        new_df = pd.DataFrame()
        new_df.to_json(sqlpath + "/" + file_name, orient="records", lines=True)
        local_file = self.obj_local.upload_parition_s3_local(
            sqlpath, copy_source, file_name, partition
        )
        assert local_file == None


class TestGetDetailsFromSql:
    """This class tests for possible testcases in get employee details from sql module"""

    def test_get_details_object(self):
        """This tests for the instance belong to the class EmployeeFromSql"""
        self.sql = EmployeeFromSql(logger_obj)
        assert isinstance(self.sql, EmployeeFromSql)

    def test_sql_server_connection_established(self, sql_server):
        """This method tests the server is connected successfully"""
        self.sql = EmployeeFromSql(logger_obj)
        client = sql_server
        conn = self.sql.sql_connection(
            client["driver"], client["host"], client["database"], client["user"], client["password"]
        )
        assert conn is not None

    @pytest.mark.xfail
    def test_sql_server_connection_not_established(self, sql_server_fail):
        """This method tests for server connection failure"""
        self.sql = EmployeeFromSql(logger_obj)
        client = sql_server_fail
        conn = self.sql.sql_connection(
            client["driver"], client["host"], client["database"], client["user"], client["password"]
        )
        assert conn is None

    def test_get_employee_details_by_joiningdate_isdone(self, sql_server, query):
        """This method tests to read from sql using pandas"""
        self.sql = EmployeeFromSql(logger_obj)
        client = sql_server
        response = self.sql.get_employee_details_by_joiningdate(
            client["driver"],
            client["host"],
            client["database"],
            client["user"],
            client["password"],
            query,
        )
        assert response is not None

    @pytest.mark.xfail
    def test_get_employee_details_by_joiningdate_is_notdone(self, sql_server_fail,query):
        """This method tests for failure case to read from sql using pandas"""
        self.sql = EmployeeFromSql(logger_obj)
        client = sql_server_fail
        resposne = self.sql.get_employee_details_by_joiningdate(
            client["driver"],
            client["host"],
            client["database"],
            client["user"],
            client["password"],
            query,
        )
        assert resposne is None

    def test_get_data_from_sql_passed_for_between(
        self, start_date, condition, end_date, table, column
    ):
        """This method tests for pulling employee details based on
        joining dates for between condition"""
        self.sql = EmployeeFromSql(logger_obj)
        get_query = self.sql.get_query_from_where_condition(
            start_date, condition, end_date, table, column
        )
        assert get_query is not None

    @pytest.mark.xfail
    def test_get_data_from_sql_failed_for_between(
        self, start_date, condition, end_date, table, column
    ):
        """This method tests failure case for pulling employee details based
        on joining dates for between condition"""
        self.obj_pullapi = EmployeeFromSql(logger_obj)
        get_query = self.sql.get_query_from_where_condition(
            start_date, condition, end_date, table, column
        )
        assert get_query is None

    def test_get_data_from_sql_passed_for_notbetween(
        self, start_date, condition_not_between, table,date_none, column
    ):
        """This method tests for pulling employee details based on
        joining dates for conditions other than between"""
        self.sql = EmployeeFromSql(logger_obj)
        get_query = self.sql.get_query_from_where_condition(
            start_date, condition_not_between,date_none, table, column
        )
        assert get_query is not None

    @pytest.mark.xfail
    def test_get_data_from_sql_failed_for_notbetween(
        self, start_date, condition_not_between, table, column,enddate
    ):
        """This method tests failure case for pulling employee details
        based on joining dates for conditions other than between"""
        self.obj_pullapi = EmployeeFromSql(logger_obj)
        get_query = self.sql.get_query_from_where_condition(
            start_date, condition_not_between,end_date, table, column
        )
        assert get_query is None


class TestEmployeeDetailsPartition:
    """This Test Class will check for all the possible Test cases in
    employee details sql partition s3 module"""

    def test_pull_metaweather_sql_objects(self, start_date, end_date, condition):
        """This tests for the instance belong to the class EmployeeDetailsPartitionS3"""
        self.obj_sqlapi = EmployeeDetailsPartitionS3(logger_obj, start_date, end_date, condition)
        assert isinstance(self.obj_sqlapi, EmployeeDetailsPartitionS3)

    def test_filter_create_response_by_date_isdone(self, start_date, end_date, condition, response):
        """This method tests for filtering the response from sql based on dates"""
        self.obj_sql = EmployeeDetailsPartitionS3(logger_obj, start_date, end_date, condition)
        result = self.obj_sql.filter_create_response_by_date(response, start_date)
        assert result == f"employee_{int(time())}.json"

    @pytest.mark.xfail
    def test_filter_create_response_by_date_is_notdone(
        self, start_date, end_date, condition, response
    ):
        """This method tests failure case for filtering the response from sql based on dates"""
        self.obj_sql = EmployeeDetailsPartitionS3(logger_obj, start_date, end_date, condition)
        result = self.obj_sql.filter_create_response_by_date(response, start_date)
        assert result == None

    def test_put_partition_path_isdone(self, start_date, end_date, condition):
        """This method tests for giving the partition path based on year,month,date"""
        self.obj_sql = EmployeeDetailsPartitionS3(logger_obj, start_date, end_date, condition)
        response = self.obj_sql.put_partition_path(start_date)
        assert response == "pt_year=2022/pt_month=05/pt_day=08"

    @pytest.mark.xfail
    def test_put_partition_path_is_notdone(self, start_date, end_date, condition):
        """This method tests failure case for giving the partition path
        based on year,month,date"""
        self.obj_sql = EmployeeDetailsPartitionS3(logger_obj, start_date, end_date, condition)
        response = self.obj_sql.put_partition_path(start_date)
        assert response is None

    def test_valid_date_is_passed(self):
        """This method tests the date is valid"""
        date = "2022-05-20"
        result = valid_date(date)
        assert result == datetime.strptime(date, "%Y-%m-%d").date()

    @pytest.mark.xfail
    def test_valid_date_is_failed(self):
        """This method tests the date is not valid"""
        result = valid_date("10-02-2000")
        assert result is None
