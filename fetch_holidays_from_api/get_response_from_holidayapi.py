"""This method get the response from holiday api for the given year and endpoints and
region based on their status codes"""
import requests
import sys

class HolidayApi:
    """This class has methods to get details from Holiday Api for the given endpoints,
    region and year"""

    def __init__(self, section, logger):
        """This is the init method for the class HolidayApi"""
        self.logger = logger
        self.section = section

    def check_availability_of_year(self, year):
        """This method checks the availability of the given year for the holiday api"""
        try:
            min_year = int(self.section["min_year"])
            max_year = int(self.section["max_year"])
            if year in range(min_year, max_year):
                available_year = year
                self.logger.info("%s is available in holiday api for getting public holidays", year)
            else:
                raise Exception
        except Exception as err:
            available_year = None
            self.logger.error(
                "%s is not available for getting public holidays from api %s", year, err
            )
            sys.exit("system terminated since given year is out of range")
        return available_year

    def get_endpoint_for_public_holidays_long_weekend(self, year, country, epoint):
        """This method gets the endpoint for fetching the public holidays and long_weekend for given year and country
        parameters: year - year on which public holiday details are needed
                    country - region for which datas are needed"""
        try:
            if epoint == "PublicHolidays":
                year = self.check_availability_of_year(year)
            endpoint = f"{epoint}/{year}/{country}"
            self.logger.info("Got %s endpoint for given %s and region %s", epoint, year, endpoint)
            response = self.get_response_from_api(endpoint)
        except Exception as err:
            self.logger.error(
                "Cannot get the %s endpoint for %s and %s %s", epoint, year, country, err
            )
            response = None
        return response

    def get_endpoint_for_next_publicholidays(self, country):
        """This method gets the endpoint for fetching the public holidays for next 365 days for
        given country, parameters:  country - region for which datas are needed"""
        try:
            print(country)
            endpoint = f"NextPublicHolidays/{country}"
            self.logger.info(
                "Got next_publicholidays endpoint for given year and region %s", endpoint
            )
            response = self.get_response_from_api(endpoint)
        except Exception as err:
            self.logger.error("Cannot get the next_publicholidays endpoint for %s %s", country, err)
            response = None
        return response

    def get_response_from_api(self, endpoint):
        """This method gets the response from api for the given endpoint
        parameters: endpoint - endpoint for requests from api"""
        try:
            base_url = self.section["basic_url"]
            request_url = base_url + endpoint
            # print(request_url)
            response = requests.get(request_url)
            if response.status_code != 200:
                self.logger.error("Cannot get the response from api with %s", response.status_code)
                response_json = None
            else:
                response_json = response.json()
                self.logger.info(
                    "Got the response from api for given year with status %s",
                    response.status_code,
                )
        except Exception as err:
            self.logger.error("Cannot get the response from api due to problem in api %s", err)
            print("No response from api", err)
            response_json = None
    
        return response_json
