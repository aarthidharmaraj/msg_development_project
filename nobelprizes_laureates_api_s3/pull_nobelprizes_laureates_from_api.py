"""This module fetches the nobel prize and laureates data from api 
"https://api.nobelprize.org/2.0/" and return the response """

import os
import configparser
from datetime import date
import requests

parent_dir = os.path.dirname(os.getcwd())
config = configparser.ConfigParser()
config.read(parent_dir + "/details.ini")


class NobelprizeLaureatesFromApi:
    """This class has methods to get response for Nobel prizes and laureates
    from the api for the given year"""

    def __init__(self, logger):
        """This is the init method for the class NobelprizeLaureatesFromApi"""
        self.logger = logger
        self.base_url = config["nobelprize_laureates_api"]["basic_url"]
        self.year_limit = int(date.today().year) - 1
        self.min_year = 1901

    def pull_nobelprizes_laureates_from_api(self, pull_for, year):
        """This method pulls the response for Nobel prizes from the api"""
        try:
            if year in range(self.min_year, self.year_limit + 1):
                endpoint = f"{pull_for}?nobelPrizeYear={year}"
                request_url = self.base_url + endpoint
                response = requests.get(request_url)
                if response.status_code == 200:
                    response = response.json()
                    self.logger.info(
                        "Got the response from api for given year with status {response.status_code}"
                    )
                else:
                    print("It gives a failure response with code", response.status_code)
                    self.logger.error(
                        f"It gives a failure response with code {response.status_code}"
                    )
                    response = None
            else:
                self.logger.error(f"Cannot fetch for the given year{year}")
                print("No responses are available for given year", year)
                response = None

        except Exception as err:
            self.logger.error(f"Cannot get the response from api due to problem in api{err}")
            print("No response from api", err)
            response = None
        return response
