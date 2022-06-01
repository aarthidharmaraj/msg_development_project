""" This module authenticate api with api key and get 
historical observations and checklist Feed from eBird Api based on given date"""
from datetime import date, datetime
import requests
import pandas as pd
from api_key_encryption import EncryptDecryptApiKey


class PullDataFromEBirdApi:
    """This class has methods to pull historical observations and checklist Feed from
    api with api key authentication based on the given date"""

    def __init__(self, logger, config):
        """This is the init method for the class PullDataFromEBirdApi"""
        self.logger = logger
        self.config = config
        self.base_url = self.config["eBird_api_datas"]["basic_url"]
        self.min_year = 1800
        self.year_limit = int(date.today().year)
        self.api_key = EncryptDecryptApiKey().decrypt_from_config()

    def authenticate_api_with_key(self):
        """This method decrypt the api key from config file and create
        the header and authenticate the api with api key"""
        try:
            # print(self.api_key)
            header = {"X-eBirdApiToken": self.api_key}
        except Exception as err:
            self.logger.error("Cannot generate the header with the given api_key %s", err)
            header = None
        return header

    def check_range_for_givendate(self, date_check):
        """This method fetches data from api for the given date and region
        parameters:
        date : the date to fetch details from api
        region code : the country on which datas to be fetched"""

        try:
            striped_date = datetime.strptime(str(date_check), "%Y-%m-%d")
            if striped_date.year in range(self.min_year, self.year_limit + 1):
                date = striped_date
            else:
                self.logger.error("No response are available for given year%s", date.year)
                print("No response are available for given year", date.year)
                date = None
        except Exception as err:
            self.logger.error("Cannot get the response or endpoint for the given date %s", err)
            date = None
        return date

    def fetch_historical_data_from_api(self, date_check, region_code):
        """This method fetches historical observations from eBirdapi for the given date and region
        parameters:
        date_check : the date to fetch details from api
        region code : the country on which datas to be fetched"""
        try:
            date = self.check_range_for_givendate(date_check)
            endpoint = f"data/obs/{region_code}/historic/{date.year}/{date.month}/{date.day}"
            print(endpoint)
            response = self.get_response_from_endpoint(endpoint)
        except Exception as err:
            self.logger.error(
                "Cannot generate the endpoint for getting historicall observations for the given date %s",
                err,
            )
            response = None
        return response

    def fetch_checklist_feed_from_api(self, date_check, region_code):
        """This method fetches Checklist feed from eBirdapi for the given date and region
        parameters:
        date_check : the date to fetch details from api
        region code : the country on which datas to be fetched"""
        try:
            date = self.check_range_for_givendate(date_check)
            endpoint = f"product/lists/{region_code}/{date.year}/{date.month}/{date.day}"
            response = self.get_response_from_endpoint(endpoint)
        except Exception as err:
            self.logger.error(
                "Cannot generate the endpoint for getting Checklist feed for the given date %s", err
            )
            response = None
        return response

    def fetch_top100_contributors_from_api(self, date_check, region_code):
        """This method fetches top100_contributors from eBirdapi for the given date and region
        parameters:
        date_check : the date to fetch details from api
        region code : the country on which datas to be fetched"""
        try:
            date = self.check_range_for_givendate(date_check)
            endpoint = f"product/top100/{region_code}/{date.year}/{date.month}/{date.day}"
            response = self.get_response_from_endpoint(endpoint)
        except Exception as err:
            self.logger.error(
                "Cannot generate the endpoint for getting top100_contributors for the given date %s",
                err,
            )
            response = None
        return response

    def get_response_from_endpoint(self, endpoint):
        """This method gets the json response from the eBirdapi for the
        given endpoint and return as a dataframe
        parameter: endpoint - endpoint for getting the response"""
        try:
            header = self.authenticate_api_with_key()
            request_url = self.base_url + endpoint
            response = requests.get(request_url, headers=header)
            if response.status_code == 200:
                response_json = response.json()
                self.logger.info(
                    f"Got the response from api for given date with status{response.status_code}"
                )
            else:
                print("Cannot get the response from api", response.status_code)
                self.logger.error("It gives a failure response with code %s", response.status_code)
                response_json = None

        except Exception as err:
            self.logger.error(f"Cannot get the response from api due to problem in api{err}")
            print("No response from api", err)
            response_json = None
        return pd.DataFrame(response_json)


# obj=PullDataFromEBirdApi("logger","config")
# obj.authenticate_api_with_key()
