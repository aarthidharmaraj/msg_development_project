"""This module gets the response from thirukual api for the given Kural number based on 
their status codes"""
import requests


class ThirukuralApi:
    """This class has methods to get data of the Kural based on the given number from
    thirukural api"""

    def __init__(self, section, logger):
        """This is the init method for the class ThirukuralApi"""
        self.logger = logger
        self.section = section

    def get_endpoint_for_thirukural_data(self, number):
        """This method gets the endpoint for fetching the data for the kural for the given number
        parameters: number - the kural number for which the datas are needed"""
        try:
            endpoint = f"?num={number}"
            self.logger.info("Got %s endpoint for given kural %s", endpoint, number)
            response = self.get_response_from_api(endpoint)
        except Exception as err:
            self.logger.error("Cannot get the endpoint for kural %s and %s", number, err)
            response = None
        return response

    def get_response_from_api(self, endpoint):
        """This method gets the response from api for the given endpoint
        parameters: endpoint - endpoint for requests from api"""
        try:
            base_url = self.section["basic_url"]
            request_url = base_url + endpoint
            print(request_url)
            response = requests.get(request_url)
            if response.status_code != 200:
                self.logger.error("Cannot get the response from api with %s", response.status_code)
                response_json = None
            else:
                response_json = response.json()
                self.logger.info(
                    "Got the response from api for given object_id with status %s",
                    response.status_code,
                )
        except Exception as err:
            self.logger.error("Cannot get the response from api due to problem in api %s", err)
            print("No response from api", err)
            response_json = None
        return response_json
