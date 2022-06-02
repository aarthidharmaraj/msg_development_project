""" This module authenticate api with api key and get historical observations and
checklist Feed from eBird Api based on given date"""
from datetime import date, datetime
import requests
from cryptography.fernet import Fernet


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

    def decrypt_api_key_from_config(self):
        """This method gets the encrypted api key from config, decrypt it and return the data"""
        try:
            encrypt_key = self.config["eBird_api_datas"]["api_key"]
            fernet_key = self.config["eBird_api_datas"]["fernet_key"]
            fernet = Fernet(fernet_key)
            self.logger.info("Got the encrypted key from config %s", encrypt_key)
            decrypt_token = fernet.decrypt(encrypt_key.encode()).decode()
            self.logger.info("Successfully decrypted the key %s", decrypt_token)
        except Exception as err:
            decrypt_token = None
            self.logger.error("Cannot decrypt the api key from config %s", err)
        return decrypt_token

    def authenticate_api_with_key(self):
        """This method decrypt the api key from config file and create
        the header and authenticate the api with api key"""
        try:
            api_key_token = self.decrypt_api_key_from_config()
            header = {"X-eBirdApiToken": api_key_token}
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
                get_date = striped_date
            else:
                self.logger.error("No response are available for given year%s", get_date.year)
                print("No response are available for given year", get_date.year)
                get_date = None
        except Exception as err:
            self.logger.error("Cannot get the response or endpoint for the given date %s", err)
            get_date = None
        return get_date

    def fetch_historical_data_from_api(self, date_check, reg_code, ep_value):
        """This method fetches historical observations from eBirdapi for the given date and region
        parameters:
        date_check : the date to fetch details from api
        region code : the country on which datas to be fetched"""
        try:
            get_date = self.check_range_for_givendate(date_check)
            endpoint = (
                f"{ep_value}/{reg_code}/historic/{get_date.year}/{get_date.month}/{get_date.day}"
            )
            response = self.get_response_from_endpoint(endpoint, reg_code)
        except Exception as err:
            self.logger.error(
                "Cannot generate the endpoint for getting historicall data on given date %s",
                err,
            )
            response = None
        return response

    def fetch_checklist_feed_top100_contributors_from_api(self, date_check, reg_code, ep_value):
        """This method fetches top100_contributors or checklist feed from eBirdapi
        for the given date and region
        parameters: date_check : the date to fetch details from api
                    reg_code : the country on which datas to be fetched
                    ep_value : the value for the endpoint"""
        try:
            get_date = self.check_range_for_givendate(date_check)
            endpoint = f"{ep_value}/{reg_code}/{get_date.year}/{get_date.month}/{get_date.day}"
            self.logger.info("Generated the endpoint for fetching datas from api")
            response = self.get_response_from_endpoint(endpoint, reg_code)
        except Exception as err:
            self.logger.error(
                "Cannot generate the endpoint for getting top100_contributors or checklist_feed for the given date %s",
                err,
            )
            response = None
        return response

    def get_response_from_endpoint(self, endpoint, region):
        """This method gets the json response from the eBirdapi for the
        given endpoint and return as a dataframe
        parameter: endpoint - endpoint for getting the response
                    region - the country on which data is needed"""
        try:
            header = self.authenticate_api_with_key()
            request_url = self.base_url + endpoint
            response = requests.get(request_url, headers=header)
            if response.status_code == 200:
                response_json = response.json()
                self.logger.info(
                    "Got the response from api for given region %s with status %s",
                    region,
                    response.status_code,
                )
            else:
                print("Cannot get the response from api", response.status_code)
                self.logger.error(
                    "It gives a failure response with code %s for region %s",
                    response.status_code,
                    region,
                )
                response_json = None

        except Exception as err:
            self.logger.error("Cannot get the response from api due to problem in api %s", err)
            print("No response from api", err)
            response_json = None
        return response_json
