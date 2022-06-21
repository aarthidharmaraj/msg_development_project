"""This module gets the response from sunrise sunset for the given date,based on their latitude
and longitude as per their status codes"""
import requests


class SunriseSunsetApi:
    """This class has methods to get data from  sunrise sunset api for the given latitude
    longitude location and date"""

    def __init__(self, section, logger):
        """This is the init method for the class SunriseSunsetApi"""
        self.logger = logger
        self.section = section

    def get_endpoint_for_sunrise_sunset(self, date, lat, long):
        """This method gets the endpoint for fetching the sunrise and sunset for the given date
        parameters: lat- latitude for which details are needed
                    long - longitude for which datas are needed
                    date - date on which the sunrise and sunset details are needed"""
        try:
            endpoint = f"?lat={lat}&lng={long}&date={date}&formatted=0"
            self.logger.info("Got %s endpoint for given %s date", endpoint, date)
            response = self.get_response_from_api(endpoint)
        except Exception as err:
            self.logger.error("Cannot get the endpoint for %s and %s", date, err)
            response = None
            print(response)
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
